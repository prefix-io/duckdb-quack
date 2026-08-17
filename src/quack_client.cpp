#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "quack_client.hpp"
#include "quack_server.hpp"
#include "quack_uri.hpp"

#include <atomic>
#include <condition_variable>
#include <thread>

namespace duckdb {
template <class T>
string GetUriPart(T ele) {
	if (ele.afterLast - ele.first < 1) {
		throw InvalidInputException("Invalid URI");
	}
	return string(ele.first, ele.afterLast - ele.first);
}

//! Resolve the optional EXTRA_HTTP_HEADERS from the `quack` secret scoped to this URI and add them
//! to `headers`. Silently does nothing when no matching secret / header map is present.
static void LoadExtraHttpHeaders(optional_ptr<ClientContext> context, DatabaseInstance &db, const QuackUri &uri,
                                 HTTPHeaders &headers) {
	auto &secret_manager = SecretManager::Get(db);
	auto transaction = context ? CatalogTransaction::GetSystemCatalogTransaction(*context)
	                           : CatalogTransaction::GetSystemTransaction(db);
	auto match = secret_manager.LookupSecret(transaction, uri.Uri(), "quack");
	if (!match.HasMatch()) {
		return;
	}
	const auto &kv = dynamic_cast<const KeyValueSecret &>(*match.secret_entry->secret);
	Value headers_value;
	if (!kv.TryGetValue("extra_http_headers", headers_value) || headers_value.IsNull()) {
		return;
	}
	for (const auto &entry : MapValue::GetChildren(headers_value)) {
		const auto &kv_pair = StructValue::GetChildren(entry);
		headers.Insert(kv_pair[0].ToString(), kv_pair[1].ToString());
	}
}

QuackClient::QuackClient(DatabaseInstance &db_p, const QuackUri &uri_p) : db(db_p), uri(uri_p) {
}

QuackClient::~QuackClient() {
}

HttpsQuackClient::HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p) : QuackClient(db, uri_p) {};

HttpsQuackClient::~HttpsQuackClient() {
}

namespace {

//! All state a parked HTTP request needs, heap-owned and shared with the worker
//! thread that runs it. When a local interrupt abandons the request, the worker
//! keeps a reference so nothing dangles; it completes into the job and exits.
struct QuackHttpJob {
	//! Pins the database (and thus HTTPUtil) for a worker that outlives its caller.
	shared_ptr<DatabaseInstance> db;
	string url;
	HTTPHeaders headers;
	unique_ptr<HTTPParams> params;
	MemoryStream request_stream;
	unique_ptr<PostRequestInfo> post;
	unique_ptr<HTTPResponse> response;
	string error;
	bool has_error = false;

	std::mutex mutex;
	std::condition_variable cv;
	bool done = false;
};

//! Client-generated identity for PREPAREs issued outside a transaction (mono's
//! quack_query path). Uniqueness per process is all the server-side staleness
//! check needs.
std::atomic<idx_t> next_generated_query_id {1};

} // namespace

unique_ptr<QuackMessage> HttpsQuackClient::RequestInternal(optional_ptr<ClientContext> context,
                                                           unique_ptr<QuackMessage> request_message) {
	D_ASSERT(request_message);

	lock_guard<mutex> guard(request_mutex);

	auto &http_util = HTTPUtil::Get(db);
	auto request_url = uri.Http() + "/quack";
	if (!extra_headers_loaded) {
		// Resolve EXTRA_HTTP_HEADERS from the quack secret once; reused for every request on this client.
		LoadExtraHttpHeaders(context, db, uri, extra_headers);
		extra_headers_loaded = true;
	}

	auto request_type = request_message->Type();

	// Inject client_query_id from context into the message before sending.
	// Guard against reading the active query during transaction start itself
	// (e.g. BEGIN TRANSACTION via QuackCatalog::ExecuteCommand), where the
	// transaction isn't yet installed on the TransactionContext.
	optional_idx client_query_id;
	if (context && context->transaction.HasActiveTransaction()) {
		auto raw_query_id = context->transaction.GetActiveQuery();
		if (raw_query_id != DConstants::INVALID_INDEX) {
			client_query_id = raw_query_id;
			request_message->SetClientQueryId(client_query_id);
		}
	}
	if (request_type == MessageType::PREPARE_REQUEST) {
		// Every query gets an identity, transaction or not: it is what a later
		// targeted CANCEL (local interrupt, operator) matches against.
		if (!client_query_id.IsValid()) {
			client_query_id = next_generated_query_id.fetch_add(1);
			request_message->SetClientQueryId(client_query_id);
		}
		if (bound_connection) {
			bound_connection->SetActivePrepareQueryId(client_query_id);
		}
	}

	auto job = make_shared_ptr<QuackHttpJob>();
	job->db = db.shared_from_this();
	job->url = request_url;
	job->headers = extra_headers;
	if (context && context->transaction.HasActiveTransaction()) {
		job->params = http_util.InitializeParameters(*context, request_url);
	} else {
		job->params = http_util.InitializeParameters(db, request_url);
	}
	job->params->timeout = request_timeout_override_seconds.IsValid()
	                           ? request_timeout_override_seconds.GetIndex()
	                           : HTTP_TIMEOUT_SECONDS;
	job->params->retries = 0;
	request_message->ToMemoryStream(job->request_stream);
	job->post = make_uniq<PostRequestInfo>(job->url, job->headers, *job->params, job->request_stream.GetData(),
	                                       job->request_stream.GetPosition());

	// Time the request
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();

	// Run the request on a detached worker so this thread can observe a local
	// interrupt: a parked request otherwise blocks until the server responds —
	// measured at the full remaining query duration (probe: 37s of a 42s CTAS).
	std::thread([job]() {
		try {
			auto &worker_http_util = HTTPUtil::Get(*job->db);
			job->response = worker_http_util.Request(*job->post);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			job->error = error.Message();
			job->has_error = true;
		} catch (...) {
			job->error = "unknown error";
			job->has_error = true;
		}
		std::lock_guard<std::mutex> job_guard(job->mutex);
		job->done = true;
		job->cv.notify_all();
	}).detach();

	{
		std::unique_lock<std::mutex> job_lock(job->mutex);
		while (!job->done) {
			job->cv.wait_for(job_lock, std::chrono::milliseconds(100));
			if (job->done) {
				break;
			}
			if (context && context->interrupted.load()) {
				job_lock.unlock();
				// Best-effort server-side cancellation before unwinding locally.
				// Only against servers that negotiated v2: a stock v1 server's
				// DISCONNECT-during-query handling corrupts its admission
				// accounting, so against v1 we only unwind locally (the query is
				// then reclaimed by lease expiry / operator action, as before).
				if (bound_connection && bound_connection->NegotiatedVersion() >= 2 &&
				    (request_type == MessageType::PREPARE_REQUEST || request_type == MessageType::FETCH_REQUEST ||
				     request_type == MessageType::APPEND_REQUEST)) {
					try {
						auto cancel_client = QuackClient::GetClient(db, uri);
						cancel_client->SetRequestTimeoutSeconds(10);
						auto cancel_message = make_uniq<CancelRequestMessage>(bound_connection->ConnectionId());
						cancel_message->SetClientQueryId(bound_connection->ActivePrepareQueryId());
						cancel_client->Request<SuccessResponse>(nullptr, std::move(cancel_message));
					} catch (...) { // NOLINT: cancellation is best-effort; the local unwind proceeds regardless
					}
				}
				throw InterruptException();
			}
		}
	}

	if (job->has_error) {
		throw IOException("Failed to send message: %s", job->error);
	}
	auto &response = job->response;
	if (!response || !response->Success()) {
		string error = response ? response->GetError() : "no response";
		throw IOException("Failed to send message: %s", error);
	}

	MemoryStream non_owning_read_stream((data_ptr_t)job->post->buffer_out.data(), job->post->buffer_out.size());
	auto response_message = QuackMessage::FromMemoryStream(non_owning_read_stream);

	// logging stuff, own scope
	{
		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();

		auto request_type = request_message->Type();
		string connection_id;
		string query;
		optional_idx client_query_id;
		switch (request_type) {
		case MessageType::PREPARE_REQUEST: {
			auto &msg = request_message->Cast<PrepareRequestMessage>();
			connection_id = msg.ConnectionId();
			query = msg.Query();
			break;
		}
		case MessageType::FETCH_REQUEST:
			connection_id = request_message->Cast<FetchRequestMessage>().ConnectionId();
			break;
		case MessageType::APPEND_REQUEST:
			connection_id = request_message->Cast<AppendRequestMessage>().ConnectionId();
			break;
		default:
			break;
		}

		// Log RPC message
		auto &logger = context ? Logger::Get(*context) : Logger::Get(db);
		if (logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL)) {
			string error;
			if (response_message->Type() == MessageType::ERROR_RESPONSE) {
				error = response_message->Cast<ErrorResponse>().ErrorMessage();
			}
			auto msg =
			    QuackLogType::ConstructLogMessage(request_type, connection_id, client_query_id, query, uri.Http(),
			                                      end_time - start_time, response_message->Type(), error);
			logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
		}
	}

	return response_message;
}

unique_ptr<QuackClient> QuackClient::GetClient(DatabaseInstance &db, const QuackUri &uri) {
	ExtensionHelper::AutoLoadExtension(db, "httpfs");
	if (!db.ExtensionIsLoaded("httpfs")) {
		throw MissingExtensionException("The rpc extension requires the httpfs extension to be loaded!");
	}

	return make_uniq<HttpsQuackClient>(db, uri);
}

unique_ptr<QuackClient> QuackClient::GetClient(ClientContext &context, const QuackUri &uri) {
	return GetClient(*context.db, uri);
}

QuackClientConnection::QuackClientConnection(unique_ptr<QuackClient> client_p, QuackUri uri_p, string connection_id_p,
                                             idx_t negotiated_version_p, idx_t max_connections_cached)
    : uri(std::move(uri_p)), connection_id(std::move(connection_id_p)), negotiated_version(negotiated_version_p),
      max_connections_cached(max_connections_cached) {
	if (client_p) {
		client_p->BindConnection(this);
		StoreClient(std::move(client_p));
	}
}

QuackClientConnection::~QuackClientConnection() {
	if (!cached_clients.empty()) {
		try {
			auto &client = cached_clients.back();
			client->Request<SuccessResponse>(nullptr, make_uniq<DisconnectMessage>(connection_id));
		} catch (...) {
		}
	}
}

shared_ptr<QuackClientConnection> QuackClient::ConnectToServer(ClientContext &context, const QuackUri &uri,
                                                               string token) {
	// if no token is provided fetch it from the secret manager
	if (token.empty()) {
		token = QuackServer::TokenFromSecret(context, uri);
	}
	if (token.empty()) {
		throw InvalidInputException("Could not find a Quack authentication token");
	}

	// open a HTTP client to the server
	auto client = QuackClient::GetClient(context, uri);

	// submit the connection request
	auto connection_request_response =
	    client->Request<ConnectionResponseMessage>(context, make_uniq<ConnectionRequestMessage>(token));
	// success! we got a connection id and the server's selected protocol version
	// construct the client connection and return it
	auto connection_id = connection_request_response->ConnectionId();
	auto negotiated_version = connection_request_response->QuackVersion();
	return make_shared_ptr<QuackClientConnection>(std::move(client), uri, std::move(connection_id),
	                                              negotiated_version);
}

unique_ptr<QuackClientWrapper> QuackClientConnection::GetClient(ClientContext &context) const {
	lock_guard<mutex> guard(lock);
	unique_ptr<QuackClient> result;
	if (!cached_clients.empty()) {
		// use client from the cache
		result = std::move(cached_clients.back());
		cached_clients.pop_back();
	} else {
		// instantiate a new client
		result = QuackClient::GetClient(context, uri);
		result->BindConnection(this);
	}
	return make_uniq<QuackClientWrapper>(std::move(result), shared_from_this());
}

void QuackClientConnection::StoreClient(unique_ptr<QuackClient> client_p) const {
	lock_guard<mutex> guard(lock);
	if (cached_clients.size() >= max_connections_cached) {
		// already exceeded max cache size
		return;
	}
	cached_clients.push_back(std::move(client_p));
}

QuackClientWrapper::QuackClientWrapper(unique_ptr<QuackClient> client_p,
                                       shared_ptr<const QuackClientConnection> client_connection_p)
    : client(std::move(client_p)), client_connection(std::move(client_connection_p)) {
}

QuackClientWrapper::~QuackClientWrapper() {
	client_connection->StoreClient(std::move(client));
}

QuackClient &QuackClientWrapper::GetClient() {
	return *client;
}

} // namespace duckdb
