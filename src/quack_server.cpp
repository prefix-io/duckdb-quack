#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/render_tree.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "quack_server.hpp"
#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_storage.hpp"

namespace duckdb {
QuackConnection::QuackConnection(string session_id_p) : session_id(std::move(session_id_p)) {
}

QuackConnection::~QuackConnection() {
}

void QuackServer::ValidateToken(const string &token) {
	if (token.size() < 4) {
		throw InvalidInputException("Quack server token must be at least 4 characters long");
	}
}

QuackServer::QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p)
    : db_ptr(context_p.db), uri(uri_p), token(token_p) {
	ValidateToken(token);
	socket_watch = make_uniq<QuackSocketWatch>(db_ptr);
	socket_watch->Start();
	lease_reaper = make_uniq<QuackLeaseReaper>(*this, db_ptr);
	lease_reaper->Start();
}

string QuackServer::TokenFromSecret(ClientContext &context, const QuackUri &uri) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// Look up by the canonical form: secret scopes are matched as plain string prefixes, so
	// `uri.Uri()` would make the match depend on how the endpoint was spelled.
	auto match = secret_manager.LookupSecret(transaction, uri.CanonicalUri(), "quack");
	if (match.HasMatch()) {
		const auto &kv = dynamic_cast<const KeyValueSecret &>(*match.secret_entry->secret);
		return kv.TryGetValue("token", true).ToString();
	}
	return "";
}

QuackServer::~QuackServer() {
	// Join the reaper before the socket watch and registry go away; it holds a
	// bare reference to this server.
	if (lease_reaper) {
		lease_reaper->Stop();
	}
	if (socket_watch) {
		socket_watch->Stop();
	}
}

vector<QuackConnectionSnapshot> QuackServer::GetActiveConnectionSnap() {
	vector<QuackConnectionSnapshot> result;
	auto now = std::chrono::steady_clock::now();
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	for (auto &conn_kv : active_connections) {
		auto &conn = conn_kv.second;
		// state_lock, not lock: the lifecycle metadata is guarded by the leaf state
		// lock precisely so snapshots never park behind an executing query.
		std::lock_guard<std::mutex> state_guard(conn->state_lock);
		QuackConnectionSnapshot snapshot;
		snapshot.session_id = conn->session_id;
		snapshot.sql_query = conn->sql_query;
		snapshot.query_state = conn->query_state;
		snapshot.query_started_at = conn->query_started_at;
		snapshot.negotiated_version = conn->negotiated_version;
		snapshot.active_client_query_id = conn->active_client_query_id;
		if (conn->negotiated_version >= 3) {
			snapshot.lease_age_seconds =
			    std::chrono::duration_cast<std::chrono::seconds>(now - conn->last_traffic).count();
		}
		result.push_back(std::move(snapshot));
	}
	return result;
}

vector<shared_ptr<QuackConnection>> QuackServer::SnapshotConnections() {
	vector<shared_ptr<QuackConnection>> result;
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	result.reserve(active_connections.size());
	for (auto &conn_kv : active_connections) {
		result.push_back(conn_kv.second);
	}
	return result;
}

shared_ptr<QuackConnection> QuackServer::GetConnection(const string &connection_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	auto it = active_connections.find(connection_id);
	if (it != active_connections.end()) {
		return it->second;
	}
	return nullptr;
}

string QuackServer::CreateNewConnection(const string &session_id, idx_t negotiated_version) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	D_ASSERT(active_connections.find(session_id) == active_connections.end());

	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	auto new_connection = make_shared_ptr<QuackConnection>(session_id);
	new_connection->duckdb_connection = make_uniq<Connection>(*db);
	new_connection->duckdb_connection->context->config.enable_progress_bar = false;
	new_connection->negotiated_version = negotiated_version;
	// new_connection->duckdb_connection->context->config.streaming_buffer_size = 10 * 1000000; // 10 MB
	active_connections[session_id] = std::move(new_connection);
	return session_id;
}

bool QuackServer::RemoveConnection(const string &session_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	auto entry = active_connections.find(session_id);
	if (entry == active_connections.end()) {
		// unknown client
		return false;
	}
	active_connections.erase(entry);
	return true;
}

void QuackServer::ReapIfLatched(QuackConnection &connection) {
	{
		std::lock_guard<std::mutex> state_guard(connection.state_lock);
		if (!connection.disconnect_latched) {
			return;
		}
		if (connection.query_state == QuackQueryState::ACTIVE ||
		    connection.query_state == QuackQueryState::CANCELLING) {
			// A query is still executing or unwinding. Leave the entry registered so
			// quack_active_connections() (and admission accounting built on it) keeps
			// seeing the work; the handler that unwinds it calls back here.
			return;
		}
	}
	RemoveConnection(connection.session_id);
}

string QuackServer::CancelActiveQuery(QuackConnection &connection, optional_idx expected_query_id,
                                      bool require_active, optional_idx expected_epoch) {
	idx_t observed_epoch;
	{
		std::lock_guard<std::mutex> state_guard(connection.state_lock);
		if (expected_epoch.IsValid() && connection.query_epoch != expected_epoch.GetIndex()) {
			// The query this cancel was aimed at already ended; a newer query on the
			// same connection must not be collateral damage.
			return string();
		}
		if (expected_query_id.IsValid() && connection.active_client_query_id.IsValid() &&
		    expected_query_id.GetIndex() != connection.active_client_query_id.GetIndex()) {
			return "Cancellation rejected: stale query identity";
		}
		if (connection.query_state != QuackQueryState::ACTIVE &&
		    connection.query_state != QuackQueryState::CANCELLING) {
			return require_active ? "Cancellation rejected: no active query" : string();
		}
		observed_epoch = connection.query_epoch;
		connection.query_state = QuackQueryState::CANCELLING;
	}

	// try_lock distinguishes the two shapes of an active query. A parked
	// PREPARE/FETCH handler holds `lock` for its whole request, so failing to
	// acquire it means a request thread is executing: interrupt and let it unwind
	// (its error path transitions the state and triggers the latched reap). Owning
	// the lock means the query is a suspended streaming result with no request in
	// flight — nothing will ever unwind it, so tear it down right here.
	std::unique_lock<std::mutex> exec_lock(connection.lock, std::try_to_lock);
	if (!exec_lock.owns_lock()) {
		connection.duckdb_connection->Interrupt();
		return string();
	}

	bool still_same_query;
	{
		std::lock_guard<std::mutex> state_guard(connection.state_lock);
		still_same_query =
		    connection.query_epoch == observed_epoch && connection.query_state == QuackQueryState::CANCELLING;
	}
	if (still_same_query) {
		connection.duckdb_query_result.reset();
		// Rotate the result UUID: a late FETCH for the torn-down result must fail
		// with "Result has been closed", not read a null result as a clean empty
		// end-of-stream and silently truncate the client's data.
		connection.result_uuid = UUID::GenerateRandomUUID();
		std::lock_guard<std::mutex> state_guard(connection.state_lock);
		if (connection.query_epoch == observed_epoch) {
			connection.query_state = QuackQueryState::CANCELLED;
		}
	}
	return string();
}

static string GetSettingString(DatabaseInstance &db, const string &setting_name) {
	Value setting_val;
	auto &config = DBConfig::GetConfig(db);

	auto lookup_result = config.TryGetCurrentSetting(setting_name, setting_val);
	D_ASSERT(lookup_result);
	D_ASSERT(setting_val.type().id() == LogicalTypeId::VARCHAR);
	auto setting_str = setting_val.GetValue<string>();
	D_ASSERT(!setting_str.empty());
	return setting_str;
}

template <typename... ARGS>
static Value EvaluateAuthQuery(DatabaseInstance &db, const string &sql, ARGS... values) {
	Connection dummy_connection(db);
	auto auth_result = dummy_connection.Query(sql, values...);
	if (!auth_result || auth_result->HasError()) {
		return Value(false);
	}
	auto auth_result_chunk = auth_result->Fetch();
	if (!auth_result_chunk || auth_result_chunk->size() == 0) {
		return Value(false);
	}
	return auth_result_chunk->GetValue(0, 0);
}

static constexpr idx_t kTokenBytes = 16; // 128 bits

static string HexEncode(const data_t *bytes, idx_t n) {
	string result(n * 2, '\0');
	for (idx_t i = 0; i < n; i++) {
		result[2 * i] = Blob::HEX_TABLE[bytes[i] >> 4];
		result[2 * i + 1] = Blob::HEX_TABLE[bytes[i] & 0x0F];
	}
	return result;
}

string QuackServer::GenerateRandomToken(DatabaseInstance &db) {
	auto encryption_util = db.GetEncryptionUtil(false);
	auto metadata =
	    make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, kTokenBytes, EncryptionTypes::EncryptionVersion::NONE);
	auto rng = encryption_util->CreateEncryptionState(std::move(metadata));

	data_t bytes[kTokenBytes];
	rng->GenerateRandomData(bytes, kTokenBytes);
	return HexEncode(bytes, kTokenBytes);
}

string QuackServer::GenerateSessionId() {
	{
		std::lock_guard<std::mutex> lock(session_id_rng_mutex);
		if (!session_id_rng) {
			auto db = db_ptr.lock();
			if (!db) {
				throw InternalException("Database was closed");
			}
			auto encryption_util = db->GetEncryptionUtil(false);
			auto metadata = make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, kTokenBytes,
			                                                   EncryptionTypes::EncryptionVersion::NONE);
			session_id_rng = encryption_util->CreateEncryptionState(std::move(metadata));
		}
	}

	data_t bytes[kTokenBytes];
	session_id_rng->GenerateRandomData(bytes, kTokenBytes);
	return HexEncode(bytes, kTokenBytes);
}

static string ExtractQuery(QuackMessage &msg) {
	if (msg.Type() == MessageType::PREPARE_REQUEST) {
		return msg.Cast<PrepareRequestMessage>().Query();
	}
	return "";
}

bool ServerSupportsMessage(MessageType type) {
	switch (type) {
	case MessageType::CONNECTION_REQUEST:
	case MessageType::PREPARE_REQUEST:
	case MessageType::FETCH_REQUEST:
	case MessageType::APPEND_REQUEST:
	case MessageType::DISCONNECT_MESSAGE:
	case MessageType::CANCEL_REQUEST:
	case MessageType::HEARTBEAT:
		return true;
	default:
		return false;
	}
}

bool MessageRequiresConnection(MessageType type) {
	switch (type) {
	case MessageType::CONNECTION_REQUEST:
		return false;
	default:
		return true;
	}
}

namespace {
//! Unregisters a socket watch on every exit path of HandleMessage.
struct SocketWatchGuard {
	SocketWatchGuard(QuackSocketWatch &watch_p, idx_t watch_id_p) : watch(watch_p), watch_id(watch_id_p) {
	}
	~SocketWatchGuard() {
		watch.Unregister(watch_id);
	}
	QuackSocketWatch &watch;
	idx_t watch_id;
};
} // namespace

static bool MessageExecutesQuery(MessageType type) {
	switch (type) {
	case MessageType::PREPARE_REQUEST:
	case MessageType::FETCH_REQUEST:
	case MessageType::APPEND_REQUEST:
		return true;
	default:
		return false;
	}
}

// main switcheroo happens here
unique_ptr<QuackMessage> QuackServer::HandleMessage(MemoryStream &read_stream,
                                                    const std::function<bool()> &connection_closed_probe) {
	auto db = db_ptr.lock();
	if (!db) {
		return make_uniq<ErrorResponse>("Database was closed");
	}
	auto &logger = Logger::Get(*db);
	bool should_log = logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL);

	int64_t start_time = 0;
	if (should_log) {
		start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                 .time_since_epoch()
		                 .count();
	}

	// start deserializing the message
	read_stream.Rewind();
	BinaryDeserializer deserializer(read_stream);

	// first read the header
	auto header = QuackMessage::DeserializeHeader(deserializer);

	// validate if the server can handle this type of message - the server cannot handle all message types
	if (!ServerSupportsMessage(header.type)) {
		return make_uniq<ErrorResponse>("Unsupported message type for server");
	}

	// if the message requires it, obtain a connection
	// these are basically all messages aside from connect request
	shared_ptr<QuackConnection> connection;
	if (MessageRequiresConnection(header.type)) {
		connection = GetConnection(header.connection_id);
		if (!connection) {
			return make_uniq<ErrorResponse>("Invalid connection id");
		}
		// A disconnect-latched connection accepts no new work; it only remains
		// registered so an unwinding query stays observable. DISCONNECT stays
		// idempotent and CANCEL may still target the unwinding query. Heartbeats
		// deliberately get the error too: latched means latched, a late beat must
		// not look like a live connection.
		{
			std::lock_guard<std::mutex> state_guard(connection->state_lock);
			if (connection->disconnect_latched) {
				if (header.type != MessageType::DISCONNECT_MESSAGE && header.type != MessageType::CANCEL_REQUEST) {
					return make_uniq<ErrorResponse>("Connection has been closed");
				}
			} else {
				// Every valid routed message renews the lease; heartbeats exist only
				// for the windows in which the client sends nothing else.
				connection->last_traffic = std::chrono::steady_clock::now();
				connection->lease_expiry_reported = false;
			}
		}
	}

	// now deserialize the actual message
	auto received_message = QuackMessage::DeserializeMessage(deserializer, header);

	// While an executing message is parked here, its client socket is watched:
	// the client can never receive this response if that socket dies, so a dead
	// socket means the query is pure waste (Layer 2 socket liveness).
	unique_ptr<SocketWatchGuard> watch_guard;
	if (connection && connection_closed_probe && socket_watch && MessageExecutesQuery(header.type)) {
		auto watch_id = socket_watch->Register(connection, connection_closed_probe);
		watch_guard = make_uniq<SocketWatchGuard>(*socket_watch, watch_id);
	}

	// process the message
	auto response = HandleMessageInternal(*db, *received_message, connection);
	watch_guard.reset();

	// reap a latched connection once its query is no longer executing; while a
	// query is active or unwinding the entry must stay visible to
	// quack_active_connections() so admission accounting never undercounts.
	if (connection) {
		ReapIfLatched(*connection);
	}

	if (should_log) {
		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();
		string error;
		if (response->Type() == MessageType::ERROR_RESPONSE) {
			error = response->Cast<ErrorResponse>().ErrorMessage();
		}
		auto msg = QuackLogType::ConstructLogMessage(header.type, header.connection_id, header.client_query_id,
		                                             ExtractQuery(*received_message), "", end_time - start_time,
		                                             response->Type(), error);
		logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
	}

	return response;
}

static vector<unique_ptr<DataChunkWrapper>> CreateBatch(Allocator &allocator, unique_ptr<QueryResult> &query_result,
                                                        idx_t max_chunks) {
	vector<unique_ptr<DataChunkWrapper>> results;

	while (results.size() < max_chunks) {
		auto result_chunk = query_result->Fetch();
		// error case
		if (!result_chunk && query_result->HasError()) {
			results.clear();
			return results;
		}
		// we are done case
		if (!result_chunk || result_chunk->size() == 0) {
			query_result.reset();
			break;
		}
		results.push_back(make_uniq<DataChunkWrapper>(*result_chunk));
	}
	return results;
}

unique_ptr<QuackMessage> QuackServer::HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
                                                            optional_ptr<QuackConnection> connection_p) {
	switch (received_message.Type()) {
	case MessageType::CONNECTION_REQUEST: {
		auto &connection_request_message = received_message.Cast<ConnectionRequestMessage>();
		auto client_min = connection_request_message.MinimumSupportedQuackVersion();
		auto client_max = connection_request_message.MaximumSupportedQuackVersion();
		if (client_min > QuackServer::MAX_QUACK_VERSION || client_max < QuackServer::QUACK_VERSION ||
		    client_min > client_max) {
			return make_uniq<ErrorResponse>("Unsupported Quack version - server supports versions %llu through %llu",
			                                QuackServer::QUACK_VERSION, QuackServer::MAX_QUACK_VERSION);
		}
		auto negotiated_version = MinValue<idx_t>(client_max, QuackServer::MAX_QUACK_VERSION);
		string session_id = GenerateSessionId();
		auto auth_result = EvaluateAuthQuery(
		    db, StringUtil::Format("SELECT %s(?, ?, ?)", GetSettingString(db, "quack_authentication_function")),
		    Value(session_id), Value(connection_request_message.AuthString()), Value(Token()));

		if (auth_result.IsNull() ||
		    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
			return make_uniq<ErrorResponse>("Authentication failed");
		}
		return make_uniq<ConnectionResponseMessage>(CreateNewConnection(session_id, negotiated_version),
		                                            negotiated_version);
	}
	case MessageType::DISCONNECT_MESSAGE: {
		auto &connection = *connection_p;
		disconnects_received++;
		{
			std::lock_guard<std::mutex> state_guard(connection.state_lock);
			if (connection.disconnect_latched) {
				// Latched means latched: repeated disconnects are idempotent successes.
				return make_uniq<SuccessResponse>();
			}
			connection.disconnect_latched = true;
		}
		// Interrupt (parked request) or tear down (suspended result) any active
		// query; an idle connection is simply reaped after this handler returns.
		// require_active=false: disconnecting an idle connection is normal.
		CancelActiveQuery(connection, optional_idx(), false);
		return make_uniq<SuccessResponse>();
	}
	case MessageType::CANCEL_REQUEST: {
		auto &connection = *connection_p;
		cancel_requests_received++;
		{
			std::lock_guard<std::mutex> state_guard(connection.state_lock);
			if (connection.negotiated_version < 2) {
				return make_uniq<ErrorResponse>("CANCEL_REQUEST requires negotiated protocol version 2 or higher");
			}
		}
		auto error = CancelActiveQuery(connection, received_message.ClientQueryId(), true);
		if (!error.empty()) {
			return make_uniq<ErrorResponse>(error);
		}
		return make_uniq<SuccessResponse>();
	}
	case MessageType::HEARTBEAT: {
		auto &connection = *connection_p;
		{
			std::lock_guard<std::mutex> state_guard(connection.state_lock);
			if (connection.negotiated_version < 3) {
				return make_uniq<ErrorResponse>("HEARTBEAT requires negotiated protocol version 3 or higher");
			}
		}
		// The lease was already renewed during message routing; a heartbeat has no
		// other effect.
		return make_uniq<SuccessResponse>();
	}
	case MessageType::PREPARE_REQUEST: {
		auto &prepare_request_message = received_message.Cast<PrepareRequestMessage>();
		auto &connection = *connection_p;

		// TODO do not do this if there is no fun set
		auto auth_result = EvaluateAuthQuery(
		    db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(db, "quack_authorization_function")),
		    Value(prepare_request_message.ConnectionId()), Value(prepare_request_message.Query()));
		if (auth_result.IsNull() ||
		    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
			return make_uniq<ErrorResponse>("Authorization failed");
		}
		auto effective_sql = (auth_result.type().id() == LogicalTypeId::VARCHAR) ? auth_result.GetValue<string>()
		                                                                         : prepare_request_message.Query();

		// Publish the query's lifecycle metadata BEFORE acquiring the execution lock:
		// from this point a cancellation can see and target the query, and the entry
		// is admission-countable for its entire life.
		idx_t my_epoch;
		{
			std::lock_guard<std::mutex> state_guard(connection.state_lock);
			my_epoch = ++connection.query_epoch;
			connection.sql_query = prepare_request_message.Query();
			connection.query_state = QuackQueryState::ACTIVE;
			connection.query_started_at = Timestamp::GetCurrentTimestamp();
			connection.active_client_query_id = prepare_request_message.ClientQueryId();
		}
		// Terminal transitions are epoch-guarded: while this handler unwinds, a
		// queued PREPARE may already have published the next query's metadata, and
		// this handler must not stomp it.
		auto transition = [&](QuackQueryState new_state, bool clear_sql) {
			std::lock_guard<std::mutex> state_guard(connection.state_lock);
			if (connection.query_epoch != my_epoch) {
				return;
			}
			connection.query_state = new_state;
			if (clear_sql) {
				connection.sql_query = "";
			}
		};

		std::unique_lock<std::mutex> lock(connection.lock);
		// A cancel or disconnect may have raced in between metadata publication and
		// lock acquisition; its interrupt would be consumed and reset by query
		// startup, so it must be honored here instead of silently lost.
		{
			std::lock_guard<std::mutex> state_guard(connection.state_lock);
			if (connection.query_epoch != my_epoch || connection.query_state != QuackQueryState::ACTIVE) {
				if (connection.query_epoch == my_epoch && connection.query_state == QuackQueryState::CANCELLING) {
					connection.query_state = QuackQueryState::CANCELLED;
				}
				return make_uniq<ErrorResponse>("Query was cancelled before execution started");
			}
		}
		connection.duckdb_query_result.reset();

		{
			auto query_result = connection.duckdb_connection->SendQuery(effective_sql);
			if (query_result->HasError()) {
				// TODO; instead of cancelled, add an ERROR state
				transition(QuackQueryState::CANCELLED, true);
				return make_uniq<ErrorResponse>(query_result->GetErrorObject());
			}
			if (query_result->names.empty()) {
				transition(QuackQueryState::CANCELLED, true);
				return make_uniq<ErrorResponse>("Query did not return any columns");
			}

			connection.duckdb_query_result = std::move(query_result);
		}
		// Fresh query → restart batch numbering. Clients' local state is re-initialized on
		// a new PREPARE, so indices start at 0 again.
		connection.next_batch_index = 1;
		// generate a random UUID to uniquely identify the result
		connection.result_uuid = UUID::GenerateRandomUUID();

		Value max_chunks_val;
		DBConfig::GetConfig(db).TryGetCurrentSetting("quack_fetch_batch_chunks", max_chunks_val);
		auto max_chunks_per_batch = max_chunks_val.GetValue<uint64_t>();

		auto names = connection.duckdb_query_result->names;
		auto types = connection.duckdb_query_result->types;

		auto results = CreateBatch(Allocator::Get(db), connection.duckdb_query_result, max_chunks_per_batch);
		if (connection.duckdb_query_result && connection.duckdb_query_result->HasError()) {
			D_ASSERT(results.empty());

			auto error_message = connection.duckdb_query_result->GetErrorObject();
			connection.duckdb_query_result.reset();
			// Includes interrupt-driven unwinds: without this transition the entry
			// stayed ACTIVE forever after any mid-stream error, permanently occupying
			// an admission slot.
			transition(QuackQueryState::CANCELLED, false);
			return make_uniq<ErrorResponse>(std::move(error_message));
		}
		auto needs_more_fetch = results.size() == max_chunks_per_batch;
		if (!needs_more_fetch) {
			transition(QuackQueryState::FINISHED, false);
		}
		return make_uniq<PrepareResponseMessage>(types, names, std::move(results), needs_more_fetch,
		                                         connection.result_uuid);
	}

	case MessageType::FETCH_REQUEST: {
		auto &fetch_request_message = received_message.Cast<FetchRequestMessage>();
		auto &connection = *connection_p;
		std::unique_lock<std::mutex> lock(connection.lock);

		// Same epoch guard as PREPARE: this fetch's state transitions apply only to
		// the query that was current when the lock was acquired.
		idx_t my_epoch;
		{
			std::lock_guard<std::mutex> state_guard(connection.state_lock);
			my_epoch = connection.query_epoch;
		}
		auto transition = [&](QuackQueryState new_state) {
			std::lock_guard<std::mutex> state_guard(connection.state_lock);
			if (connection.query_epoch == my_epoch) {
				connection.query_state = new_state;
			}
		};

		if (connection.result_uuid != fetch_request_message.uuid) {
			return make_uniq<ErrorResponse>("Result has been closed");
		}
		if (!connection.duckdb_query_result) {
			return make_uniq<FetchResponseMessage>();
		}
		if (connection.duckdb_query_result->HasError()) {
			transition(QuackQueryState::CANCELLED);
			return make_uniq<ErrorResponse>(connection.duckdb_query_result->GetErrorObject());
		}

		Value max_chunks_val;
		DBConfig::GetConfig(db).TryGetCurrentSetting("quack_fetch_batch_chunks", max_chunks_val);
		auto max_chunks_per_batch = max_chunks_val.GetValue<uint64_t>();

		auto results = CreateBatch(Allocator::Get(db), connection.duckdb_query_result, max_chunks_per_batch);
		if (connection.duckdb_query_result && connection.duckdb_query_result->HasError()) { // TODO this is duplicated
			D_ASSERT(results.empty());
			auto error_message = connection.duckdb_query_result->GetErrorObject();
			connection.duckdb_query_result.reset();
			// Interrupt-driven and ordinary mid-stream errors alike must leave a
			// terminal state, or the entry occupies an admission slot forever.
			transition(QuackQueryState::CANCELLED);
			return make_uniq<ErrorResponse>(std::move(error_message));
		}
		auto assigned_batch_index = connection.next_batch_index++;
		if (results.size() < max_chunks_per_batch) {
			transition(QuackQueryState::FINISHED);
		}
		return make_uniq<FetchResponseMessage>(std::move(results), optional_idx(assigned_batch_index));
	}

	case MessageType::APPEND_REQUEST: {
		auto &append_request_message = received_message.Cast<AppendRequestMessage>();
		auto &connection = *connection_p;

		// we never execute this query, but throw it at the authorization function so it can check if this user gets to
		// insert into this table
		auto dummy_insert_query =
		    StringUtil::Format("INSERT INTO %s.%s VALUES (NULL)", SQLIdentifier(append_request_message.SchemaName()),
		                       SQLIdentifier(append_request_message.TableName()));

		// TODO do not do this if there is no fun set
		{
			auto auth_result = EvaluateAuthQuery(
			    db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(db, "quack_authorization_function")),
			    Value(append_request_message.ConnectionId()), Value(dummy_insert_query));
			if (auth_result.IsNull() ||
			    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
				return make_uniq<ErrorResponse>("Authorization failed");
			}
		}

		std::unique_lock<std::mutex> lock(connection.lock);
		auto &context = *connection.duckdb_connection->context;
		auto table_info = context.TableInfo(append_request_message.SchemaName(), append_request_message.TableName());
		if (!table_info) {
			return make_uniq<ErrorResponse>("Table %s.%s does not exist",
			                                SQLIdentifier(append_request_message.SchemaName()),
			                                SQLIdentifier(append_request_message.TableName()));
		}
		try {
			ColumnDataCollection collection(Allocator::Get(context), append_request_message.AppendChunk().GetTypes());
			collection.Append(append_request_message.AppendChunk());
			connection.duckdb_connection->Append(*table_info, collection);
		} catch (std::exception &ex) {
			// apend failed - directly pass error to user
			return make_uniq<ErrorResponse>(ErrorData(ex));
		}
		return make_uniq<SuccessResponse>();
	}
	default: {
		return make_uniq<ErrorResponse>(
		    StringUtil::Format("Unimplemented message type %s", MessageTypeToString(received_message.Type())));
	}
	}
}
} // namespace duckdb
