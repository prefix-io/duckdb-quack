#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_uri.hpp"

namespace duckdb {
class QuackClientConnection;
struct QuackClientWrapper;

class QuackClient {
public:
	explicit QuackClient(DatabaseInstance &db_p, const QuackUri &uri_p);
	virtual ~QuackClient();

	template <class TARGET>
	unique_ptr<TARGET> Request(optional_ptr<ClientContext> context, unique_ptr<QuackMessage> request_message) {
		auto response_message = RequestInternal(context, std::move(request_message));
		if (response_message->Type() != TARGET::TYPE) {
			if (response_message->Type() == MessageType::ERROR_RESPONSE) {
				// if we get an error throw it immediately
				response_message->Cast<ErrorResponse>().Error().Throw();
			}
			throw IOException("Expected %s message, got %s instead", MessageTypeToString(TARGET::TYPE),
			                  MessageTypeToString(response_message->Type()));
		}
		return unique_ptr_cast<QuackMessage, TARGET>(std::move(response_message));
	}

	static unique_ptr<QuackClient> GetClient(DatabaseInstance &db, const QuackUri &uri);
	static unique_ptr<QuackClient> GetClient(ClientContext &context, const QuackUri &uri);

	static shared_ptr<QuackClientConnection> ConnectToServer(ClientContext &context, const QuackUri &uri, string token);

	//! Associate this client with a logical connection. Enables the interrupt path:
	//! when a local interrupt fires while a request is parked, a client bound to a
	//! connection that negotiated protocol v2 sends a targeted CANCEL for the
	//! connection's active query before unwinding.
	void BindConnection(const QuackClientConnection *connection_p) {
		bound_connection = connection_p;
	}

	//! Bound per-request timeout override (seconds). Used by the best-effort CANCEL
	//! path so it cannot park for the full default request timeout.
	void SetRequestTimeoutSeconds(idx_t seconds) {
		request_timeout_override_seconds = seconds;
	}

protected:
	mutex request_mutex;
	MemoryStream read_stream, write_stream;
	DatabaseInstance &db;
	QuackUri uri;
	//! Back-pointer to the owning logical connection (null for connection-less
	//! clients, e.g. the CONNECTION_REQUEST itself or a one-shot CANCEL client).
	//! Cached clients are owned by the connection, so the pointer cannot dangle
	//! while a request is in flight.
	const QuackClientConnection *bound_connection = nullptr;
	optional_idx request_timeout_override_seconds;

private:
	virtual unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                                 unique_ptr<QuackMessage> request_message) = 0;
};

class QuackClientConnection : public enable_shared_from_this<QuackClientConnection> {
public:
	explicit QuackClientConnection(unique_ptr<QuackClient> client_p, QuackUri uri_p, string connection_id_p,
	                               idx_t negotiated_version_p = 1, idx_t max_connections_cached = 1);
	~QuackClientConnection();

	const string &ConnectionId() const {
		return connection_id;
	}
	const QuackUri &ServerURI() const {
		return uri;
	}
	idx_t NegotiatedVersion() const {
		return negotiated_version;
	}

	//! Identity of the connection's active query as stamped on its PREPARE header.
	//! Read by the interrupt path so a CANCEL issued while a FETCH is parked still
	//! targets the query the server recorded at PREPARE time.
	optional_idx ActivePrepareQueryId() const {
		auto raw = active_prepare_query_id.load();
		return raw == DConstants::INVALID_INDEX ? optional_idx() : optional_idx(raw);
	}
	void SetActivePrepareQueryId(optional_idx query_id) const {
		active_prepare_query_id.store(query_id.IsValid() ? query_id.GetIndex() : DConstants::INVALID_INDEX);
	}

	//! Get a client (either a cached one, or open a new one if required)
	unique_ptr<QuackClientWrapper> GetClient(ClientContext &context) const;
	//! Return a client back to the cache
	void StoreClient(unique_ptr<QuackClient> client_p) const;

private:
	QuackUri uri;
	string connection_id;
	idx_t negotiated_version;
	mutable atomic<idx_t> active_prepare_query_id {DConstants::INVALID_INDEX};
	mutable mutex lock;
	mutable vector<unique_ptr<QuackClient>> cached_clients;
	idx_t max_connections_cached;
};

struct QuackClientWrapper {
	QuackClientWrapper(unique_ptr<QuackClient> client, shared_ptr<const QuackClientConnection> client_connection);
	~QuackClientWrapper();

	QuackClient &GetClient();

private:
	unique_ptr<QuackClient> client;
	shared_ptr<const QuackClientConnection> client_connection;
};

class HttpsQuackClient : public QuackClient {
public:
	static constexpr uint64_t HTTP_TIMEOUT_SECONDS = 86400;

	HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p);
	~HttpsQuackClient() override;

private:
	unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                         unique_ptr<QuackMessage> request_message) override;

private:
	//! Extra HTTP headers resolved once from the `quack` secret (EXTRA_HTTP_HEADERS),
	//! injected into every request. Loaded lazily on the first request.
	HTTPHeaders extra_headers;
	bool extra_headers_loaded = false;
};

} // namespace duckdb
