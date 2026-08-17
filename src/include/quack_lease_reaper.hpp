#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

class QuackServer;
class DatabaseInstance;

enum class QuackLeaseMode : uint8_t { OFF, OBSERVE, ENFORCE };

//! Lease/heartbeat backstop (Layer 3). Every valid protocol message renews a
//! connection's lease; a v3 client's background thread additionally heartbeats
//! (~every 20s) when it would otherwise be silent. A lease that lapses therefore
//! means the client process is gone — the one abandonment class Layers 1-2 cannot
//! see, because between requests there is no socket to watch (a client killed
//! between FETCHes leaves an ACTIVE registry entry and a suspended QueryResult
//! behind forever).
//!
//! Modes (global setting `quack_lease`):
//!   off     — no sweeping
//!   observe — count and log expiries, never reap (rollout default)
//!   enforce — latch the connection closed (reusing the disconnect latch, so a
//!             late heartbeat cannot revive it), cancel its active query, and
//!             reap the registry entry
//!
//! Scope: only connections that negotiated protocol >= 3. Older clients cannot
//! heartbeat, so their silence proves nothing; they are never lease-reaped.
class QuackLeaseReaper {
public:
	//! Lease duration; a client heartbeats at ~LEASE/3 so a healthy client can
	//! miss two beats before its lease lapses.
	static constexpr const idx_t LEASE_DURATION_MS = 60000;
	static constexpr const idx_t REAP_INTERVAL_MS = 15000;

	QuackLeaseReaper(QuackServer &server_p, weak_ptr<DatabaseInstance> db_p);
	~QuackLeaseReaper();

	void Start();
	void Stop();

	idx_t ExpiredDetected() const {
		return expired_detected.load();
	}
	idx_t ExpiredReaped() const {
		return expired_reaped.load();
	}

	static QuackLeaseMode CurrentMode(const weak_ptr<DatabaseInstance> &db_ptr);

private:
	void ReapLoop();
	void Sweep(QuackLeaseMode mode);
	void LogExpiry(const string &connection_id, int64_t lease_age_ms, QuackLeaseMode mode, bool reaped);

	//! The owning server; safe as a bare reference because the server joins this
	//! thread in its destructor, before the connection registry is torn down.
	QuackServer &server;
	weak_ptr<DatabaseInstance> db_ptr;
	std::mutex mutex;
	std::thread reap_thread;
	std::condition_variable cv;
	bool started = false;
	bool stop_requested = false;
	std::atomic<idx_t> expired_detected {0};
	std::atomic<idx_t> expired_reaped {0};
};

} // namespace duckdb
