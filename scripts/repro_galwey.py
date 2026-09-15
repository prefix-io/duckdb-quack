#!/usr/bin/env python3
"""Reproduce galwey/prod: an operator cancel that leaves a query stuck in 'cancelling'.

galwey/prod 2026-09-14: `quack_cancel_connection` returned success at 19:15, moved the
read to 'cancelling', and the query ran until the warehouse was restarted 2h17m later.
Three reap paths signalled; none landed.

Hypothesis under test — `CancelActiveQuery`'s two shapes:

    std::unique_lock<std::mutex> exec_lock(connection.lock, std::try_to_lock);
    if (!exec_lock.owns_lock()) {
      connection.duckdb_connection->Interrupt();   // "let it unwind"
      return string();
    }

  try_lock FAILS  -> a request thread holds `lock`; we Interrupt() and trust it to unwind.
  try_lock WINS   -> suspended stream, torn down here.

  If the holding thread is parked writing result frames to a socket nobody is reading, it
  is NOT inside DuckDB execution. Interrupt() has nothing to interrupt, the thread never
  unwinds, and the state stays CANCELLING forever. That is the galwey/prod signature.

Staging it needs a REAL client process frozen mid-fetch (SIGSTOP), which is why the
in-process harnesses miss it: they either drain or never fetch, so no request is parked.

Usage: python3 scripts/repro_galwey.py [--repo build/release/repository]
"""

import argparse
import os
import signal
import subprocess
import sys
import threading
import time

import duckdb

PORT = 9643
TOKEN = "repro-galwey-token"
_CANCEL_DEADLINE = 45.0
# Wide and expensive enough that the server keeps producing frames for minutes, so the
# stream is genuinely in flight when the client freezes.
_SQL = "SELECT id, md5(md5(k || id::VARCHAR)) AS v FROM t, range(24) r"


def open_connection(repo: str) -> duckdb.DuckDBPyConnection:
  con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
  con.execute(f"FORCE INSTALL httpfs FROM '{repo}'")
  con.execute(f"FORCE INSTALL quack FROM '{repo}'")
  con.execute("LOAD httpfs; LOAD quack")
  return con


def _rows(server, lock):
  with lock:
    return server.execute(
      "SELECT connection_id::VARCHAR, state FROM quack_active_connections() "
      "WHERE state IN ('active','cancelling') AND query NOT LIKE '%quack_active_connections%'"
    ).fetchall()


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

  client = subprocess.Popen(
    [sys.executable, "scripts/stall_client.py", str(PORT), TOKEN, repo, _SQL],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
  )
  try:
    # Wait for frames to actually be flowing before freezing the peer.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
      line = client.stdout.readline()
      if "STALL_CLIENT_FETCHING" in line:
        break
      if client.poll() is not None:
        print("client exited early:", client.stderr.read()[:400])
        return 2
    else:
      print("client never started fetching")
      return 2

    time.sleep(1.5)
    os.kill(client.pid, signal.SIGSTOP)
    print(f"client {client.pid} FROZEN mid-fetch (SIGSTOP)")
    time.sleep(2.0)

    active = [r for r in _rows(server, lock) if r[1] == "active"]
    if not active:
      print("no active query after freezing the client — nothing to cancel")
      return 2
    connection_id = active[0][0]
    print(f"server has an active query on {connection_id}; issuing operator cancel")

    with lock:
      server.execute("SELECT quack_cancel_connection(?)", [connection_id]).fetchall()

    deadline = time.monotonic() + _CANCEL_DEADLINE
    last = None
    while time.monotonic() < deadline:
      rows = _rows(server, lock)
      if not rows:
        print(f"\nPASS — cancel landed in {_CANCEL_DEADLINE - (deadline - time.monotonic()):.1f}s")
        return 0
      last = rows[0]
      time.sleep(0.25)

    print(f"\n*** REPRODUCED *** query still present after {_CANCEL_DEADLINE:.0f}s, state={last[1]}")
    print("    This is the galwey/prod signature: the cancel was accepted, the state moved,")
    print("    and the execution thread never unwound.")
    return 1
  finally:
    try:
      os.kill(client.pid, signal.SIGCONT)
    except ProcessLookupError:
      pass
    client.kill()
    client.wait(timeout=10)


if __name__ == "__main__":
  sys.exit(main())
