#include "quack_active_connections.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"

#include "quack_startstop.hpp"
#include "quack_storage.hpp"

namespace duckdb {

static string QueryStateToString(QuackQueryState state) {
	switch (state) {
	case QuackQueryState::IDLE:
		return "idle";
	case QuackQueryState::CANCELLING:
		return "cancelling";
	case QuackQueryState::ACTIVE:
		return "active";
	case QuackQueryState::FINISHED:
		return "finished";
	case QuackQueryState::CANCELLED:
		return "cancelled";
	default:
		return "unknown";
	}
}

struct QuackActiveConnectionsData : FunctionData {
	bool finished = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackActiveConnectionsData>();
		result->finished = finished;
		return result;
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};

static unique_ptr<FunctionData> QuackActiveConnectionsBind(ClientContext &, TableFunctionBindInput &,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::TIMESTAMP, LogicalType::BIGINT,
	                LogicalType::BIGINT,  LogicalType::VARCHAR,   LogicalType::BIGINT};
	names = {"server_id",        "connection_id",   "query",       "state",       "query_started_at",
	         "protocol_version", "client_query_id", "lease_state", "lease_age_seconds"};
	return make_uniq<QuackActiveConnectionsData>();
}

static string LeaseStateToString(const QuackConnectionSnapshot &snap) {
	if (snap.negotiated_version < 3) {
		// Pre-heartbeat protocol: the connection carries no lease.
		return "n/a";
	}
	return snap.lease_age_seconds >= static_cast<int64_t>(QuackLeaseReaper::LEASE_DURATION_MS / 1000) ? "expired"
	                                                                                                  : "live";
}

static void QuackActiveConnectionsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<QuackActiveConnectionsData>();
	if (data.finished) {
		return;
	}

	auto snapshots = QuackStorageExtensionInfo::GetState(*context.db).GetActiveConnectionSnaps();

	idx_t row = 0;
	for (auto &snap : snapshots) {
		output.SetValue(0, row, snap.server_id);
		output.SetValue(1, row, snap.session_id);
		output.SetValue(2, row, snap.sql_query);
		output.SetValue(3, row, Value(QueryStateToString(snap.query_state)));
		if (snap.query_state == QuackQueryState::IDLE) {
			output.SetValue(4, row, Value(LogicalType::TIMESTAMP));
		} else {
			output.SetValue(4, row, Value::TIMESTAMP(snap.query_started_at));
		}
		output.SetValue(5, row, Value::BIGINT(NumericCast<int64_t>(snap.negotiated_version)));
		if (snap.active_client_query_id.IsValid()) {
			output.SetValue(6, row, Value::BIGINT(NumericCast<int64_t>(snap.active_client_query_id.GetIndex())));
		} else {
			output.SetValue(6, row, Value(LogicalType::BIGINT));
		}
		output.SetValue(7, row, Value(LeaseStateToString(snap)));
		if (snap.lease_age_seconds >= 0) {
			output.SetValue(8, row, Value::BIGINT(snap.lease_age_seconds));
		} else {
			output.SetValue(8, row, Value(LogicalType::BIGINT));
		}
		row++;
	}
	output.SetCardinality(row);
	data.finished = true;
}

TableFunction QuacktivityFunction::GetFunction() {
	return TableFunction("quack_active_connections", {}, QuackActiveConnectionsScan, QuackActiveConnectionsBind);
}

static void QuackCancelConnectionImpl(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &db = *state.GetContext().db;
	auto count = args.size();
	UnifiedVectorFormat connection_ids;
	args.data[0].ToUnifiedFormat(count, connection_ids);
	UnifiedVectorFormat query_ids;
	if (args.ColumnCount() > 1) {
		args.data[1].ToUnifiedFormat(count, query_ids);
	}

	auto result_data = FlatVector::GetData<bool>(result);
	for (idx_t row = 0; row < count; row++) {
		auto id_idx = connection_ids.sel->get_index(row);
		if (!connection_ids.validity.RowIsValid(id_idx)) {
			throw InvalidInputException("quack_cancel_connection: connection_id must not be NULL");
		}
		auto connection_id = UnifiedVectorFormat::GetData<string_t>(connection_ids)[id_idx].GetString();
		optional_idx expected_query_id;
		if (args.ColumnCount() > 1) {
			auto query_idx = query_ids.sel->get_index(row);
			if (query_ids.validity.RowIsValid(query_idx)) {
				expected_query_id = NumericCast<idx_t>(UnifiedVectorFormat::GetData<int64_t>(query_ids)[query_idx]);
			}
		}
		QuackStorageExtensionInfo::GetState(db).CancelConnection(connection_id, expected_query_id);
		result_data[row] = true;
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet QuackCancelConnectionFunction::GetFunctions() {
	ScalarFunctionSet set("quack_cancel_connection");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN, QuackCancelConnectionImpl));
	set.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::BOOLEAN, QuackCancelConnectionImpl));
	return set;
}

} // namespace duckdb
