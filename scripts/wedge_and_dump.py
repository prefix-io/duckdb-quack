#!/usr/bin/env python3
"""Wedge a query via the client-interrupt path, then dump native stacks.

Reproduces: a live client prepares a streaming query, never fetches, and interrupts.
The interrupt-cancel block in quack_client.cpp only runs while a request is parked in
`job->cv.wait_for`, so an idle client's interrupt sends nothing and the server keeps
executing. State stays 'active' forever.

Leaves the server wedged and prints its PID so gdb can show which thread is stuck and
where. That frame is the thing every black-box hypothesis so far has been guessing at.

Usage: python3 scripts/wedge_and_dump.py [--repo build/release/repository]
"""

import argparse
import os
import subprocess
import sys
import threading
import time

import duckdb

PORT = 9647
TOKEN = "wedge-token"
_SQL = "SELECT id, md5(md5(k || id::VARCHAR)) AS v FROM t, range(24) r"


def open_connection(repo: str) -> duckdb.DuckDBPyConnection:
  con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
  con.execute(f"FORCE INSTALL httpfs FROM '{repo}'")
  con.execute(f"FORCE INSTALL quack FROM '{repo}'")
  con.execute("LOAD httpfs; LOAD quack")
  return con


def main() -> int:
  parser = argparse.ArgumentParser()
  parser.add_argument("--repo", default="build/release/repository")
  args = parser.parse_args()
  repo = os.path.abspath(args.repo)

  server = open_connection(repo)
  server.execute("SET GLOBAL quack_socket_liveness = 'enforce'")
  server.execute("SET GLOBAL quack_lease = 'enforce'")
  server.execute("CREATE TABLE t AS SELECT range AS id, md5(range::VARCHAR) AS k FROM range(3000000)")
  server.execute(f"CALL quack_serve('quack:localhost:{PORT}', token := '{TOKEN}', disable_ssl := true)")
  lock = threading.Lock()

  client = open_connection(repo)
  client.execute("SET http_retries = 0")

  def run() -> None:
    try:
      client.execute(
        f"SELECT * FROM quack_query('quack:localhost:{PORT}', '{_SQL}', token := '{TOKEN}', disable_ssl := true)"
      )
      # Never fetch: no request is parked, so the interrupt path never runs.
    except Exception as exc:
      print(f"client raised: {type(exc).__name__}: {str(exc)[:100]}")

  worker = threading.Thread(target=run, daemon=True)
  worker.start()

  deadline = time.monotonic() + 30
  while time.monotonic() < deadline:
    with lock:
      rows = server.execute(
        "SELECT connection_id::VARCHAR, state FROM quack_active_connections() "
        "WHERE state = 'active' AND query NOT LIKE '%quack_active_connections%'"
      ).fetchall()
    if rows:
      break
    time.sleep(0.2)
  else:
    print("query never became active")
    return 2

  print(f"active on {rows[0][0]}; interrupting the idle client", flush=True)
  client.interrupt()
  time.sleep(5)

  with lock:
    state = server.execute(
      "SELECT state, count(*) FROM quack_active_connections() "
      "WHERE query NOT LIKE '%quack_active_connections%' GROUP BY 1"
    ).fetchall()
  print(f"after interrupt: {state}", flush=True)
  if not any(s[0] in ("active", "cancelling") for s in state):
    print("not wedged — nothing to dump")
    return 0

  # gdb must run from OUTSIDE this process: attaching to self stops every thread,
  # including the one waiting on gdb, and deadlocks.
  print(f"WEDGED_PID {os.getpid()}", flush=True)
  print("holding the wedge open for an external debugger", flush=True)
  time.sleep(240)
  return 1


if __name__ == "__main__":
  sys.exit(main())
