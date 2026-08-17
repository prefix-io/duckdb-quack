#pragma once

namespace duckdb {

class TableFunction;
class ScalarFunctionSet;

class QuacktivityFunction {
public:
	static TableFunction GetFunction();
};

//! quack_cancel_connection(connection_id [, client_query_id]) — server-local
//! operator cancellation of a connection's active query. Runs inside the serving
//! process (not over the wire); errors on unknown connections, stale query
//! identities, and idle connections rather than cancelling the wrong work.
class QuackCancelConnectionFunction {
public:
	static ScalarFunctionSet GetFunctions();
};

} // namespace duckdb
