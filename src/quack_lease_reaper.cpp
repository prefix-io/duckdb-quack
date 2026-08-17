#include "quack_lease_reaper.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

#include "quack_log.hpp"
#include "quack_message.hpp"
#include "quack_server.hpp"

namespace duckdb {

QuackLeaseReaper::QuackLeaseReaper(QuackServer &server_p, weak_ptr<DatabaseInstance> db_p)
    : server(server_p), db_ptr(std::move(db_p)) {
}

QuackLeaseReaper::~QuackLeaseReaper() {
	Stop();
}

void QuackLeaseReaper::Start() {
	std::lock_guard<std::mutex> guard(mutex);
	if (started) {
		return;
	}
	started = true;
	stop_requested = false;
	reap_thread = std::thread([this]() { ReapLoop(); });
}

void QuackLeaseReaper::Stop() {
	{
		std::lock_guard<std::mutex> guard(mutex);
		if (!started) {
			return;
		}
		stop_requested = true;
		cv.notify_all();
	}
	if (reap_thread.joinable()) {
		reap_thread.join();
	}
	std::lock_guard<std::mutex> guard(mutex);
	started = false;
}

QuackLeaseMode QuackLeaseReaper::CurrentMode(const weak_ptr<DatabaseInstance> &db_ptr) {
	auto db = db_ptr.lock();
	if (!db) {
		return QuackLeaseMode::OFF;
	}
	Value setting_val;
	if (!DBConfig::GetConfig(*db).TryGetCurrentSetting("quack_lease", setting_val)) {
		return QuackLeaseMode::OFF;
	}
	auto mode = StringUtil::Lower(setting_val.GetValue<string>());
	if (mode == "off") {
		return QuackLeaseMode::OFF;
	}
	if (mode == "enforce") {
		return QuackLeaseMode::ENFORCE;
	}
	// Unknown values degrade to observe: visible in logs, never destructive.
	return QuackLeaseMode::OBSERVE;
}

void QuackLeaseReaper::LogExpiry(const string &connection_id, int64_t lease_age_ms, QuackLeaseMode mode, bool reaped) {
	auto db = db_ptr.lock();
	if (!db) {
		return;
	}
	auto &logger = Logger::Get(*db);
	if (!logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL)) {
		return;
	}
	auto detail = StringUtil::Format("lease expired for connection %s: no traffic for %lldms (mode=%s%s)",
	                                 connection_id, lease_age_ms, mode == QuackLeaseMode::ENFORCE ? "enforce" : "observe",
	                                 reaped ? ", connection reaped" : "");
	auto msg = QuackLogType::ConstructLogMessage(MessageType::INVALID, connection_id, optional_idx(), "", "", 0,
	                                             MessageType::INVALID, detail);
	logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
}

void QuackLeaseReaper::ReapLoop() {
	while (true) {
		{
			std::unique_lock<std::mutex> guard(mutex);
			cv.wait_for(guard, std::chrono::milliseconds(REAP_INTERVAL_MS), [this]() { return stop_requested; });
			if (stop_requested) {
				return;
			}
		}
		auto mode = CurrentMode(db_ptr);
		if (mode == QuackLeaseMode::OFF) {
			continue;
		}
		Sweep(mode);
	}
}

void QuackLeaseReaper::Sweep(QuackLeaseMode mode) {
	auto now = std::chrono::steady_clock::now();
	for (auto &connection : server.SnapshotConnections()) {
		int64_t lease_age_ms;
		{
			std::lock_guard<std::mutex> state_guard(connection->state_lock);
			if (connection->negotiated_version < 3) {
				// Pre-heartbeat clients: silence proves nothing, never lease-reap.
				continue;
			}
			if (connection->disconnect_latched) {
				// Already latched (disconnect or an earlier sweep); the teardown
				// machinery owns it from here.
				continue;
			}
			lease_age_ms =
			    std::chrono::duration_cast<std::chrono::milliseconds>(now - connection->last_traffic).count();
			if (lease_age_ms < static_cast<int64_t>(LEASE_DURATION_MS)) {
				continue;
			}
			if (connection->lease_expiry_reported && mode != QuackLeaseMode::ENFORCE) {
				// Observe mode: this expiry was already counted; renewal re-arms it.
				continue;
			}
			connection->lease_expiry_reported = true;
			if (mode == QuackLeaseMode::ENFORCE) {
				// Latch under state_lock so the expiry is atomic with respect to
				// message routing: from here on every message (heartbeats included)
				// gets "Connection has been closed" — a late beat cannot revive it.
				connection->disconnect_latched = true;
			}
		}
		expired_detected++;
		bool reaped = false;
		if (mode == QuackLeaseMode::ENFORCE) {
			// Same teardown as DISCONNECT: a parked request is interrupted and its
			// unwinding handler reaps; a suspended streaming result is torn down
			// here and the reap below erases the entry. require_active=false — an
			// idle expired connection is normal.
			QuackServer::CancelActiveQuery(*connection, optional_idx(), false);
			server.ReapIfLatched(*connection);
			expired_reaped++;
			reaped = true;
		}
		LogExpiry(connection->session_id, lease_age_ms, mode, reaped);
	}
}

} // namespace duckdb
