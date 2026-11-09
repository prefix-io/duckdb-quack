#pragma once

#include "duckdb/storage/storage_extension.hpp"

#include "quack_server.hpp"

namespace duckdb {

class DatabaseInstance;

class QuackStorageExtension : public StorageExtension {
public:
	QuackStorageExtension();
};

class QuackStorageExtensionInfo : public StorageExtensionInfo {
public:
	static QuackStorageExtensionInfo &GetState(const DatabaseInstance &instance);

	QuackServer &CreateServer(ClientContext &context, const QuackUri &listen_uri, const string &token);
	//! Stop the server: close the listener, cancel its connections' active queries,
	//! then destroy it. With `wait`, the destruction runs on the calling thread, so
	//! the listener, the httplib worker pool, the lease reaper and the socket watch
	//! have all been joined when this returns. Without it, it runs on a detached
	//! thread. `wait` is refused for a statement sent over the server being stopped:
	//! the pool join would wait on the very request that is running it.
	bool StopServer(ClientContext &context, const QuackUri &listen_uri, bool wait = false);

	struct ServerSnapshot {
		string listen_uri;
		string listen_url;
		string host;
		uint16_t port;
		idx_t active_connections;
		vector<std::pair<string, string>> info;
	};

	vector<QuackConnectionSnapshot> GetActiveConnectionSnaps();
	vector<ServerSnapshot> ListServers();

	//! Cancel the active query on `connection_id` across all served endpoints.
	//! Throws on an unknown connection; otherwise surfaces CancelActiveQuery's
	//! stale/no-active errors verbatim.
	void CancelConnection(const string &connection_id, optional_idx expected_query_id);

	static constexpr const char *STORAGE_EXTENSION_KEY = "quack";

private:
	std::mutex servers_mutex;
	unordered_map<string, unique_ptr<QuackServer>> servers;
};
} // namespace duckdb
