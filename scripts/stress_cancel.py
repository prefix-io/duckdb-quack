#!/usr/bin/env python3
"""Cancel/complete race stress test.

One process hosts both ends: a serving DuckDB connection (quack_serve, socket
liveness enforced) and client connections issuing queries against it. Every
iteration races a query against a cancellation issued at a random point via a
randomly chosen path:

  - operator cancel (quack_cancel_connection on the serving connection)
  - client interrupt (duckdb interrupt() -> targeted CANCEL over the wire)
  - no cancellation at all (pure completion, keeps the race window honest)

Pass criteria: every iteration ends in a result or a clean error within the
timeout, the server keeps answering, and the registry is empty at the end.

Usage: python3 scripts/stress_cancel.py [iterations] [--repo <extension repo dir>]
"""

import argparse
import random
import sys
import threading
import time

import duckdb

PORT = 9639
TOKEN = "stress-token"


def open_connection(repo: str) -> duckdb.DuckDBPyConnection:
  con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
  con.execute(f"FORCE INSTALL httpfs FROM '{repo}'")
  con.execute(f"FORCE INSTALL quack FROM '{repo}'")
  con.execute("LOAD httpfs; LOAD quack")
  return con


def main() -> int:
  parser = argparse.ArgumentParser()
  parser.add_argument("iterations", nargs="?", type=int, default=1000)
  parser.add_argument("--repo", default="build/release/repository")
  args = parser.parse_args()

  server = open_connection(args.repo)
  server.execute("SET GLOBAL quack_socket_liveness = 'enforce'")
  server.execute("CREATE TABLE t AS SELECT range AS id FROM range(2000000)")
  server.execute(f"CALL quack_serve('quack:localhost:{PORT}', token := '{TOKEN}', disable_ssl := true)")
  server_lock = threading.Lock()

  outcomes = {"completed": 0, "cancelled": 0, "other_error": 0}
  failures: list[str] = []

  start = time.monotonic()
  for i in range(args.iterations):
    # Vary query cost so cancellations land before, during, and after execution.
    rows = random.choice([1_000, 50_000, 500_000, 2_000_000])
    sql = f"SELECT sum(id * id) FROM t WHERE id < {rows}"
    client = open_connection(args.repo)
    client.execute("SET http_retries = 0")

    result: dict = {}

    def run() -> None:
      try:
        client.execute(
          f"SELECT * FROM quack_query('quack:localhost:{PORT}', '{sql}', token := '{TOKEN}', disable_ssl := true)"
        )
        result["value"] = client.fetchall()
      except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"

    worker = threading.Thread(target=run)
    worker.start()

    mode = random.choice(["operator", "interrupt", "none"])
    if mode != "none":
      time.sleep(random.uniform(0.0, 0.015))
      if mode == "operator":
        try:
          with server_lock:
            server.execute(
              "SELECT quack_cancel_connection(connection_id) FROM quack_active_connections() WHERE state = 'active'"
            ).fetchall()
        except Exception:
          # Losing the race to completion (no active query) is expected.
          pass
      else:
        client.interrupt()

    worker.join(timeout=15)
    if worker.is_alive():
      failures.append(f"iteration {i}: HANG (mode={mode}, rows={rows})")
      print(failures[-1], flush=True)
      break

    if "value" in result:
      outcomes["completed"] += 1
    elif "Interrupted" in result.get("error", "") or "cancelled" in result.get("error", "").lower():
      outcomes["cancelled"] += 1
    else:
      outcomes["other_error"] += 1
      failures.append(f"iteration {i}: unexpected error (mode={mode}): {result.get('error')}")

    client.close()

    if (i + 1) % 100 == 0:
      with server_lock:
        active = server.execute("SELECT count(*) FROM quack_active_connections()").fetchone()[0]
      print(f"[{time.monotonic() - start:7.1f}s] {i + 1}/{args.iterations} {outcomes} registry={active}", flush=True)

  # The server must still answer, and no registry entries may linger once
  # clients are gone (their destructors sent DISCONNECT).
  time.sleep(1)
  with server_lock:
    leftover = server.execute(
      "SELECT count(*) FROM quack_active_connections() WHERE state IN ('active', 'cancelling')"
    ).fetchone()[0]
  if leftover:
    failures.append(f"{leftover} active/cancelling registry entries leaked")

  print(f"outcomes: {outcomes}")
  if failures:
    print("FAILURES:")
    for failure in failures[:20]:
      print(f"  {failure}")
    return 1
  print("PASS")
  return 0


if __name__ == "__main__":
  sys.exit(main())
