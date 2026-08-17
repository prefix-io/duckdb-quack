#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

struct QuackConnection;
class DatabaseInstance;

enum class QuackSocketLivenessMode : uint8_t { OFF, OBSERVE, ENFORCE };

//! Watches the sockets of parked PREPARE/FETCH/APPEND requests — the Postgres
//! client_connection_check_interval model. A request that is executing a query
//! parks its HTTP socket for the full duration; if that socket dies (client
//! SIGKILL/OOM sends FIN, keepalive timeout surfaces silent death), the client
//! can never receive the result, so continuing to execute is pure waste.
//!
//! Modes (global setting `quack_socket_liveness`):
//!   off     — no probing
//!   observe — log detections, never cancel (rollout default)
//!   enforce — log and cancel the connection's active query
//!
//! Scope: only sockets carrying a mid-execution request. An idle session's
//! socket closing is never treated as abandonment — logical sessions span many
//! HTTP connections (httplib closes idle keep-alives after 10s by design).
//!
//! Known limitation: under TLS a graceful close_notify leaves buffered bytes
//! that make the MSG_PEEK aliveness check report "alive"; abrupt death (the
//! case this layer exists for) is detected identically with and without TLS.
class QuackSocketWatch {
public:
	static constexpr const idx_t WATCH_INTERVAL_MS = 2000;

	explicit QuackSocketWatch(weak_ptr<DatabaseInstance> db_p);
	~QuackSocketWatch();

	//! Register a parked request's connection-closed probe. Returns a watch id
	//! for Unregister. The probe must only be called while the request is
	//! registered — the socket fd it captures is owned by the parked request's
	//! worker thread and stays open exactly that long.
	idx_t Register(shared_ptr<QuackConnection> connection, std::function<bool()> connection_closed_probe);
	void Unregister(idx_t watch_id);

	void Start();
	void Stop();

	idx_t DetectedCount() const {
		return detected_count.load();
	}
	idx_t CancelledCount() const {
		return cancelled_count.load();
	}

private:
	void WatchLoop();
	QuackSocketLivenessMode CurrentMode();
	void LogDetection(const string &connection_id, QuackSocketLivenessMode mode, bool cancelled);

	struct WatchEntry {
		weak_ptr<QuackConnection> connection;
		std::function<bool()> connection_closed_probe;
		//! A dead socket is acted on once per parked request; the unwind may take
		//! a few cycles and repeated interrupts/logs add nothing.
		bool reported = false;
	};

	weak_ptr<DatabaseInstance> db_ptr;
	std::mutex mutex;
	unordered_map<idx_t, WatchEntry> entries;
	idx_t next_watch_id = 1;
	std::thread watch_thread;
	std::condition_variable cv;
	bool started = false;
	bool stop_requested = false;
	std::atomic<idx_t> detected_count {0};
	std::atomic<idx_t> cancelled_count {0};
};

} // namespace duckdb
