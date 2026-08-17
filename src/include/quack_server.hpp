#pragma once

#include <thread>

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include "quack_uri.hpp"

#include "httplib.hpp" // TODO forward declare

namespace duckdb {

class ClientContext;
class QuackMessage;
class Connection;
class MemoryStream;
class QueryResult;
class DatabaseInstance;
class PreparedStatement;
class EncryptionState;

enum class QuackQueryState : uint8_t { IDLE, ACTIVE, CANCELLING, FINISHED, CANCELLED };

struct QuackConnection {
	explicit QuackConnection(string session_id_p);
	~QuackConnection();

	//! Serializes query execution: held by a PREPARE/FETCH/APPEND handler for the full
	//! (potentially very long) duration of its parked request. Guards duckdb_connection,
	//! duckdb_query_result, next_batch_index and result_uuid.
	mutex lock;
	unique_ptr<Connection> duckdb_connection;
	unique_ptr<QueryResult> duckdb_query_result;
	//! Monotonic counter assigned per FETCH batch — enables order-preserving parallel scans on
	idx_t next_batch_index = 1;
	//! Current result UUID
	hugeint_t result_uuid;
	string session_id;

	//! Leaf lock guarding the lifecycle metadata below. Never acquire any other lock
	//! while holding it; `lock` (execution) and the registry mutex may be held when
	//! taking it, never the reverse. Cancellation and snapshot paths must use this and
	//! must NOT take `lock` — a parked query holds `lock` until it completes, so taking
	//! it would park the cancel behind the very query it is trying to kill.
	mutex state_lock;
	string sql_query;
	QuackQueryState query_state = QuackQueryState::IDLE;
	timestamp_t query_started_at {0};
	//! Client-generated identity of the currently active query (from the message
	//! header); used to reject stale targeted cancellations.
	optional_idx active_client_query_id;
	//! Incremented at every PREPARE. Lets a cancellation verify that the query it
	//! observed is still the query it is about to tear down (completion races).
	idx_t query_epoch = 0;
	//! Latched by DISCONNECT. Once set the connection accepts no new work and is
	//! reaped from the registry as soon as any in-flight query unwinds. Latched
	//! means latched — it is never cleared.
	bool disconnect_latched = false;
	//! Protocol version negotiated at CONNECTION_REQUEST (min of client max and
	//! server max). Version-gated behavior (targeted cancel, future lease expiry)
	//! applies only to connections that negotiated >= 2.
	idx_t negotiated_version = 1;
};

struct QuackConnectionSnapshot {
	string server_id;
	string session_id;
	string sql_query;
	QuackQueryState query_state = QuackQueryState::IDLE;
	timestamp_t query_started_at {0};
	idx_t negotiated_version = 1;
};

class QuackServer {
public:
	static constexpr const idx_t QUACK_VERSION = 1;
	//! Highest protocol version this build speaks. v2 adds targeted cancellation
	//! (CANCEL_REQUEST); the selected version is min(client max, MAX_QUACK_VERSION).
	static constexpr const idx_t MAX_QUACK_VERSION = 2;

public:
	explicit QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p);
	virtual ~QuackServer();

	//! Stop accepting new connections (close the listener socket) without
	//! joining listener threads. Safe to call from a request-handler thread —
	//! does not wait on httplib's task-queue, which would deadlock when the
	//! caller is itself a worker.
	virtual void StopAccepting() {};

	//! Synchronously stop accepting connections and join the listener threads.
	//! Must NOT be called from a worker / request-handler thread; httplib's
	//! listen-loop teardown joins all workers, which would deadlock.
	virtual void Close() {};

	shared_ptr<QuackConnection> GetConnection(const string &connection_id);
	string CreateNewConnection(const string &session_id, idx_t negotiated_version);
	bool RemoveConnection(const string &session_id);
	//! Reap a disconnect-latched connection once no query is in flight. Called after
	//! every handled message so the entry stays observable (and admission-countable)
	//! until its query actually unwinds.
	void ReapIfLatched(QuackConnection &connection);

	//! Cancel the connection's active query. Never blocks on the execution lock: a
	//! parked request is interrupted and unwinds on its own; a suspended streaming
	//! result is torn down directly (with its result UUID rotated so a late FETCH
	//! fails loudly instead of reading an empty end-of-stream). Returns an error
	//! string, or empty on success. `expected_query_id`, when valid, must match the
	//! active query's client_query_id or the cancel is rejected as stale.
	//! `require_active` distinguishes CANCEL (error when idle) from DISCONNECT
	//! (idle is fine).
	string CancelActiveQuery(QuackConnection &connection, optional_idx expected_query_id, bool require_active);

	string GenerateSessionId();

	//! Generate a fresh CSPRNG-backed 128-bit token, hex-encoded (32 chars).
	static string GenerateRandomToken(DatabaseInstance &db);

	//! Throw InvalidInputException if `token` doesn't meet requirements(currently, length >= 4)
	static void ValidateToken(const string &token);

	//! Look up a quack secret matching `uri` and return its token, or an empty
	//! string when no secret matches. Lets a token be sourced from the secret
	//! manager when the caller didn't supply one explicitly.
	static string TokenFromSecret(ClientContext &context, const QuackUri &uri);

	vector<QuackConnectionSnapshot> GetActiveConnectionSnap();

	const string &Token() {
		return token;
	}

	const QuackUri &ListenUri() const {
		return uri;
	}

	idx_t ActiveConnectionCount() {
		std::lock_guard<std::mutex> lock(active_connections_mutex);
		return active_connections.size();
	}

protected:
	unique_ptr<QuackMessage> HandleMessage(MemoryStream &read_stream);
	unique_ptr<QuackMessage> HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
	                                               optional_ptr<QuackConnection> connection);

protected:
	std::vector<std::thread> listen_threads;

	weak_ptr<DatabaseInstance> db_ptr;
	mutex active_connections_mutex;
	unordered_map<string, shared_ptr<QuackConnection>> active_connections;

	mutex session_id_rng_mutex;
	shared_ptr<EncryptionState> session_id_rng;

private:
	QuackUri uri;
	string token;
};

class HttpQuackServer : public QuackServer {
public:
	HttpQuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p);

	void StopAccepting() override;
	void Close() override;

	~HttpQuackServer() override;

private:
	static void ListenThread(HttpQuackServer *server, const string &listen_host, int listen_port);

	unique_ptr<QuackMessage> ReadMessage(MemoryStream &read_stream);

	unique_ptr<duckdb_httplib::Server> server;
	bool is_running = false;
};

} // namespace duckdb
