#!/usr/bin/env python3
"""quack_stop probe: a stop ends the server's threads and the queries it was running.

This process hosts the server; a client subprocess runs a statement that never
finishes on its own (a 10^12-row join) through quack_query. Once the server
shows it active, the probe stops the server and checks:

  - the stop returns within 5 s,
  - the client's statement ends in an error, not a result, within 15 s,
  - with wait := true, this process's thread count is at most JOIN_LAG (4) over
    its pre-serve baseline when the stop returns (the kernel drops a joined thread's
    /proc entry a moment after join() returns) and back at it within 200 ms,
  - without wait, the same happens within 10 s (the destruction runs on a
    detached thread, so the count is polled),
  - quack_server_list() is empty.

Thread counts read /proc/self/task, so the probe refuses to run without /proc.

Usage: python3 scripts/probe_stop.py [--repo <extension repo dir>]
"""

import argparse
import os
import subprocess
import sys
import textwrap
import time

import duckdb

PORT = 9641
TOKEN = "probe-token"
URI = f"quack:localhost:{PORT}"
JOIN_LAG = 4
NEVER_ENDS = "SELECT count(*) FROM range(1000000) a, range(1000000) b WHERE a.range + b.range = -1"


def threads() -> int:
  return len(os.listdir("/proc/self/task"))


def open_connection(repo: str) -> duckdb.DuckDBPyConnection:
  con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
  con.execute(f"FORCE INSTALL httpfs FROM '{repo}'")
  con.execute(f"FORCE INSTALL quack FROM '{repo}'")
  con.execute("LOAD httpfs; LOAD quack")
  return con


def start_client(repo: str) -> subprocess.Popen:
  script = textwrap.dedent(f"""
    import duckdb, sys
    con = duckdb.connect(":memory:", config={{"allow_unsigned_extensions": "true"}})
    con.execute("FORCE INSTALL httpfs FROM '{repo}'")
    con.execute("FORCE INSTALL quack FROM '{repo}'")
    con.execute("LOAD httpfs; LOAD quack")
    con.execute("SET http_retries = 0")
    try:
      rows = con.execute(
        "SELECT * FROM quack_query('{URI}', '{NEVER_ENDS}', token := '{TOKEN}', disable_ssl := true)"
      ).fetchall()
    except Exception as exc:
      print(f"error: {{type(exc).__name__}}: {{exc}}", flush=True)
      sys.exit(0)
    print(f"result: {{rows}}", flush=True)
    sys.exit(0)
  """)
  return subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def wait_active(server: duckdb.DuckDBPyConnection, timeout: float) -> bool:
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    if server.execute("SELECT count(*) FROM quack_active_connections() WHERE state = 'active'").fetchone()[0]:
      return True
    time.sleep(0.05)
  return False


def scenario(server: duckdb.DuckDBPyConnection, repo: str, wait: bool, failures: list[str]) -> None:
  name = "wait := true" if wait else "default"
  baseline = threads()
  server.execute(f"CALL quack_serve('{URI}', token := '{TOKEN}', disable_ssl := true)")
  serving = threads()
  client = start_client(repo)
  try:
    if not wait_active(server, 30):
      failures.append(f"{name}: the client's statement never became active")
      return
    started = time.monotonic()
    status = server.execute(f"FROM quack_stop('{URI}'{', wait := true' if wait else ''})").fetchone()[0]
    stop_seconds = time.monotonic() - started
    after_stop = threads()
    if stop_seconds > 5:
      failures.append(f"{name}: quack_stop took {stop_seconds:.1f} s")
    if not status.startswith("Stopped listening"):
      failures.append(f"{name}: quack_stop answered {status!r}")
    if wait:
      # join() returns once the kernel clears the thread's tid, a moment before its
      # /proc/self/task entry goes, so the threads joined last (the last worker,
      # the listener, the lease reaper, the socket watch) can still be listed: at
      # most JOIN_LAG over at the return, none over 200 ms later. A detached
      # teardown shows the whole pool (~130 over) at the return.
      if after_stop > baseline + JOIN_LAG:
        failures.append(f"{name}: {after_stop} threads when the stop returned, baseline {baseline}")
      deadline = time.monotonic() + 0.2
      while threads() > baseline and time.monotonic() < deadline:
        time.sleep(0.005)
      if threads() > baseline:
        failures.append(f"{name}: {threads()} threads 200 ms after the stop returned, baseline {baseline}")
    if not wait:
      deadline = time.monotonic() + 10
      while threads() > baseline and time.monotonic() < deadline:
        time.sleep(0.05)
      if threads() > baseline:
        failures.append(f"{name}: {threads()} threads 10 s after the stop, baseline {baseline}")
    try:
      output, _ = client.communicate(timeout=15)
    except subprocess.TimeoutExpired:
      failures.append(f"{name}: the client's statement still running 15 s after the stop")
      return
    outcome = output.strip().splitlines()[0] if output.strip() else "(no output)"
    if not outcome.startswith("error:"):
      failures.append(f"{name}: the client's statement did not end in an error: {output.strip()[-300:]}")
    listed = server.execute("SELECT count(*) FROM quack_server_list()").fetchone()[0]
    if listed:
      failures.append(f"{name}: quack_server_list() has {listed} servers after the stop")
    print(
      f"{name}: baseline {baseline}, serving {serving}, at stop return {after_stop}, "
      f"stop {stop_seconds:.2f} s, client: {outcome[:120]}",
      flush=True,
    )
  finally:
    if client.poll() is None:
      client.kill()
      client.wait()


def main() -> int:
  parser = argparse.ArgumentParser()
  parser.add_argument("--repo", default="build/release/repository")
  args = parser.parse_args()

  if not os.path.isdir("/proc/self/task"):
    print("probe_stop counts threads in /proc/self/task; this platform has no /proc", file=sys.stderr)
    return 2

  server = open_connection(args.repo)
  failures: list[str] = []
  scenario(server, args.repo, wait=True, failures=failures)
  scenario(server, args.repo, wait=False, failures=failures)

  if failures:
    print("FAILURES:")
    for failure in failures:
      print(f"  {failure}")
    return 1
  print("PASS")
  return 0


if __name__ == "__main__":
  sys.exit(main())
