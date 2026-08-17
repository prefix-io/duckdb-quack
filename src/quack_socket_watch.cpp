#include "quack_socket_watch.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

#include "quack_log.hpp"
#include "quack_server.hpp"

#include <chrono>

namespace duckdb {

QuackSocketWatch::QuackSocketWatch(weak_ptr<DatabaseInstance> db_p) : db_ptr(std::move(db_p)) {
}

QuackSocketWatch::~QuackSocketWatch() {
	Stop();
}

idx_t QuackSocketWatch::Register(shared_ptr<QuackConnection> connection,
                                 std::function<bool()> connection_closed_probe) {
	std::lock_guard<std::mutex> guard(mutex);
	auto watch_id = next_watch_id++;
	entries.emplace(watch_id, WatchEntry {std::move(connection), std::move(connection_closed_probe)});
	return watch_id;
}

void QuackSocketWatch::Unregister(idx_t watch_id) {
	std::lock_guard<std::mutex> guard(mutex);
	entries.erase(watch_id);
}

void QuackSocketWatch::Start() {
	std::lock_guard<std::mutex> guard(mutex);
	if (started) {
		return;
	}
	started = true;
	stop_requested = false;
	watch_thread = std::thread([this]() { WatchLoop(); });
}

void QuackSocketWatch::Stop() {
	{
		std::lock_guard<std::mutex> guard(mutex);
		if (!started) {
			return;
		}
		stop_requested = true;
		cv.notify_all();
	}
	if (watch_thread.joinable()) {
		watch_thread.join();
	}
	std::lock_guard<std::mutex> guard(mutex);
	started = false;
}

QuackSocketLivenessMode QuackSocketWatch::CurrentMode() {
	auto db = db_ptr.lock();
	if (!db) {
		return QuackSocketLivenessMode::OFF;
	}
	Value setting_val;
	if (!DBConfig::GetConfig(*db).TryGetCurrentSetting("quack_socket_liveness", setting_val)) {
		return QuackSocketLivenessMode::OFF;
	}
	auto mode = StringUtil::Lower(setting_val.GetValue<string>());
	if (mode == "off") {
		return QuackSocketLivenessMode::OFF;
	}
	if (mode == "enforce") {
		return QuackSocketLivenessMode::ENFORCE;
	}
	// Unknown values degrade to observe: visible in logs, never destructive.
	return QuackSocketLivenessMode::OBSERVE;
}

void QuackSocketWatch::LogDetection(const string &connection_id, QuackSocketLivenessMode mode, bool cancelled) {
	auto db = db_ptr.lock();
	if (!db) {
		return;
	}
	auto &logger = Logger::Get(*db);
	if (!logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL)) {
		return;
	}
	auto detail = StringUtil::Format("socket liveness: client socket dead for connection %s (mode=%s%s)",
	                                 connection_id, mode == QuackSocketLivenessMode::ENFORCE ? "enforce" : "observe",
	                                 cancelled ? ", query cancelled" : "");
	auto msg = QuackLogType::ConstructLogMessage(MessageType::INVALID, connection_id, optional_idx(), "", "", 0,
	                                             MessageType::INVALID, detail);
	logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
}

void QuackSocketWatch::WatchLoop() {
	while (true) {
		struct Victim {
			shared_ptr<QuackConnection> connection;
			idx_t watch_id;
		};
		vector<Victim> victims;
		{
			std::unique_lock<std::mutex> guard(mutex);
			cv.wait_for(guard, std::chrono::milliseconds(WATCH_INTERVAL_MS), [this]() { return stop_requested; });
			if (stop_requested) {
				return;
			}
			auto mode = CurrentMode();
			if (mode == QuackSocketLivenessMode::OFF) {
				continue;
			}
			for (auto &entry_kv : entries) {
				auto &entry = entry_kv.second;
				if (entry.reported) {
					continue;
				}
				// Cheap zero-timeout select + MSG_PEEK: true only on FIN/RST/error,
				// never for a healthy client that is simply waiting.
				if (!entry.connection_closed_probe()) {
					continue;
				}
				auto connection = entry.connection.lock();
				if (!connection) {
					continue;
				}
				entry.reported = true;
				victims.push_back(Victim {std::move(connection), entry_kv.first});
			}
		}

		if (victims.empty()) {
			continue;
		}
		auto mode = CurrentMode();
		for (auto &victim : victims) {
			auto &connection = *victim.connection;
			detected_count++;
			bool cancelled = false;
			if (mode == QuackSocketLivenessMode::ENFORCE) {
				// Capture the parked query's identity so a completion race can never
				// redirect the cancel at a newer query on the same connection.
				optional_idx expected_epoch;
				{
					std::lock_guard<std::mutex> state_guard(connection.state_lock);
					if (connection.query_state == QuackQueryState::ACTIVE ||
					    connection.query_state == QuackQueryState::CANCELLING) {
						expected_epoch = connection.query_epoch;
					}
				}
				if (expected_epoch.IsValid()) {
					QuackServer::CancelActiveQuery(connection, optional_idx(), false, expected_epoch);
					cancelled = true;
					cancelled_count++;
				}
			}
			LogDetection(connection.session_id, mode, cancelled);
		}
	}
}

} // namespace duckdb
