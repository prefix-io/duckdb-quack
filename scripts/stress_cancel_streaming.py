#!/usr/bin/env python3
"""Cancel a LONG-RUNNING, RESULT-STREAMING query — the galwey/prod shape.

`stress_cancel.py` races cancels against completion and passes. It cannot catch the
production failure, because both of its axes are pinned away from it:

  * it cancels 0-15ms in; galwey/prod was cancelled ~45 minutes in
  * its queries are `SELECT sum(...)` — one row, no result streaming; galwey/prod
    was `SELECT <14 columns> ... LIMIT 10001`, streaming frames back to a client
  * its client is always draining; galwey/prod's client had been parked past its
    own 900s HTTP timeout without unwinding

On galwey/prod 2026-09-14 a read sat in 'cancelling' for 6,720 seconds after an
operator cancel returned success. Three reap paths signalled and none landed.

Each scenario here holds the cancel constant and varies one of those axes. A PASS
means that axis is not the trigger and can be eliminated; a HANG is the bug.

Usage: python3 scripts/stress_cancel_streaming.py [--repo <dir>] [--iterations N]
"""

import argparse
import sys
import threading
import time

import duckdb

PORT = 9641
TOKEN = "stream-stress-token"
# Long enough that a cancel lands mid-execution rather than racing the plan.
_SETTLE_SECONDS = 2.0
# galwey/prod's cancel had not landed after 6,720s. The hardening plan alerts at 10s.
_CANCEL_DEADLINE = 30.0


def open_connection(repo: str) -> duckdb.DuckDBPyConnection:
  con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
  con.execute(f"FORCE INSTALL httpfs FROM '{repo}'")
  con.execute(f"FORCE INSTALL quack FROM '{repo}'")
  con.execute("LOAD httpfs; LOAD quack")
  return con


def _active(server, server_lock) -> list:
  with server_lock:
    return server.execute(
      "SELECT connection_id::VARCHAR, state, query FROM quack_active_connections() "
      "WHERE state IN ('active', 'cancelling') AND query NOT LIKE '%quack_active_connections%'"
    ).fetchall()


def _wait_active(server, server_lock, timeout=30.0):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    rows = [r for r in _active(server, server_lock) if r[1] == "active"]
    if rows:
      return rows[0]
    time.sleep(0.1)
  return None


def _wait_gone(server, server_lock, timeout=_CANCEL_DEADLINE):
  """True if the query left BOTH 'active' and 'cancelling'.

  Checking only 'active' reports a wedged cancel as reclaimed — the blind spot that
  made galwey/prod look clear while it burned CPU and blocked checkpointing.
  """
  deadline = time.monotonic() + timeout
  last = None
  while time.monotonic() < deadline:
    rows = _active(server, server_lock)
    if not rows:
      return True, None
    last = rows[0]
    time.sleep(0.2)
  return False, last


def scenario(name, server, server_lock, repo, *, sql, settle, drain, cancel):
  """Run one query, cancel it, and report whether the cancel landed."""
  client = open_connection(repo)
  client.execute("SET http_retries = 0")
  result: dict = {}

  def run() -> None:
    try:
      client.execute(
        f"SELECT * FROM quack_query('quack:localhost:{PORT}', '{sql}', token := '{TOKEN}', disable_ssl := true)"
      )
      if drain:
        result["rows"] = len(client.fetchall())
      else:
        # Do NOT drain. The server has frames to hand back and nobody taking them —
        # the closest thing to a client parked past its own timeout.
        result["rows"] = "not drained"
    except Exception as exc:
      result["error"] = f"{type(exc).__name__}: {str(exc)[:120]}"

  worker = threading.Thread(target=run, daemon=True)
  worker.start()

  active = _wait_active(server, server_lock)
  if active is None:
    # Never a pass. A scenario that did not run tested nothing, and reporting it as
    # green is the exact failure this whole investigation keeps tripping over.
    print(f"  {name:38} SKIP  (query never became active — scenario did not run)")
    return None
  time.sleep(settle)

  if cancel == "operator":
    try:
      with server_lock:
        server.execute("SELECT quack_cancel_connection(?)", [active[0]]).fetchall()
    except Exception as exc:
      print(f"  {name:38} cancel rejected: {exc}")
  elif cancel == "interrupt":
    client.interrupt()

  landed, stuck = _wait_gone(server, server_lock)
  worker.join(timeout=5)
  if landed:
    print(f"  {name:38} PASS  (cancel landed; client: {result})")
    return True
  print(f"  {name:38} *** STUCK *** state={stuck[1]} after {_CANCEL_DEADLINE:.0f}s")
  print(f"      query: {str(stuck[2])[:100]}")
  print(f"      client: {result}")
  return False


def main() -> int:
  parser = argparse.ArgumentParser()
  parser.add_argument("--repo", default="build/release/repository")
  args = parser.parse_args()

  server = open_connection(args.repo)
  server.execute("SET GLOBAL quack_socket_liveness = 'enforce'")
  server.execute("SET GLOBAL quack_lease = 'enforce'")
  server.execute("CREATE TABLE t AS SELECT range AS id, md5(range::VARCHAR) AS k FROM range(3000000)")
  server.execute(f"CALL quack_serve('quack:localhost:{PORT}', token := '{TOKEN}', disable_ssl := true)")
  server_lock = threading.Lock()

  # Runs for minutes: real per-row work, so a cancel lands mid-execution.
  long_scalar = "SELECT max(k) FROM (SELECT md5(id::VARCHAR || k) AS k FROM t, range(40) r)"
  # Streams many rows back and keeps producing them. A LIMIT fills instantly and the
  # query is gone before a cancel can land, which is why the first attempt skipped.
  streaming = "SELECT id, md5(md5(k || id::VARCHAR)) AS v FROM t, range(8) r"

  print("baseline — what stress_cancel.py already covers")
  ok = [
    scenario(
      "scalar, early cancel, draining",
      server,
      server_lock,
      args.repo,
      sql=long_scalar,
      settle=0.0,
      drain=True,
      cancel="operator",
    ),
  ]

  print("\nvarying the axes stress_cancel.py pins away from galwey/prod")
  ok.append(
    scenario(
      "scalar, LATE cancel (settled)",
      server,
      server_lock,
      args.repo,
      sql=long_scalar,
      settle=_SETTLE_SECONDS,
      drain=True,
      cancel="operator",
    )
  )
  ok.append(
    scenario(
      "STREAMING result, draining",
      server,
      server_lock,
      args.repo,
      sql=streaming,
      settle=_SETTLE_SECONDS,
      drain=True,
      cancel="operator",
    )
  )
  ok.append(
    scenario(
      "STREAMING result, NOT draining",
      server,
      server_lock,
      args.repo,
      sql=streaming,
      settle=_SETTLE_SECONDS,
      drain=False,
      cancel="operator",
    )
  )
  ok.append(
    scenario(
      "STREAMING, DRAINING, interrupt",
      server,
      server_lock,
      args.repo,
      sql=streaming,
      settle=_SETTLE_SECONDS,
      drain=True,
      cancel="interrupt",
    )
  )
  ok.append(
    scenario(
      "STREAMING, not draining, interrupt",
      server,
      server_lock,
      args.repo,
      sql=streaming,
      settle=_SETTLE_SECONDS,
      drain=False,
      cancel="interrupt",
    )
  )

  print()
  skipped = sum(1 for r in ok if r is None)
  stuck = sum(1 for r in ok if r is False)
  if stuck:
    print(f"REPRODUCED — {stuck} cancel(s) did not land; see the STUCK line(s) above")
    return 1
  if skipped:
    print(f"INCONCLUSIVE — {skipped} scenario(s) never ran, so they eliminated nothing")
    return 2
  print("PASS — none of these axes reproduces the galwey/prod wedge")
  return 0


if __name__ == "__main__":
  sys.exit(main())
