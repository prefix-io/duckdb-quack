#!/usr/bin/env python3
"""Layer 3 lease probes: a real server (this process) + real client subprocesses.

Run after `GEN=ninja make release`:

    python3 scripts/probe_lease.py                 # all scenarios (~6 min, slow-consumer dominates)
    python3 scripts/probe_lease.py kill-enforce    # one scenario

Scenarios (see HACKING.md):
    kill-enforce   SIGKILL a client between FETCHes under quack_lease=enforce ->
                   the suspended-result connection is reaped within 75s (lease 60s + sweep 15s).
                   This is the Canyon leak Layers 1-2 cannot see.
    kill-observe   same kill under observe -> lease_expired_detected increments,
                   the entry stays registered (and lease_state reads 'expired').
    slow-consumer  a live client pauses 3x the lease between FETCHes under enforce ->
                   its background heartbeats keep the lease live and the query completes.
    sigstop        a client SIGSTOPped for 30s (half the lease) under enforce ->
                   resumes and completes; a stopped-but-alive client is never a false positive.
    old-client     a v2 client (the released v1.5-abandonment.1 extension) idles past the
                   lease under enforce -> never reaped: pre-heartbeat clients carry no lease.

Server-side state is asserted through quack_active_connections()/quack_server_list().
"""

import argparse
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time

import duckdb

PORT = 9641
TOKEN = "probe-token"
LEASE_S = 60
SWEEP_S = 15
REAP_DEADLINE_S = LEASE_S + SWEEP_S + 15  # lease + sweep + slack

REPO = "build/release/repository"
# The published pre-heartbeat (protocol v2) client, as fetched by mono's quack_artifact.
OLD_EXTENSION = pathlib.Path.home() / ".cache/prefix-quack/v1.5-abandonment.1"

CLIENT_TEMPLATE = r"""
import sys, time
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
@INSTALL@
con.execute("LOAD quack")
con.execute("LOAD httpfs")
con.execute("SET http_retries = 0")
cur = con.execute(
    "SELECT * FROM quack_query('quack:localhost:%d', 'SELECT id FROM t', token := '%s', disable_ssl := true)"
)
rows = 0
first = True
while True:
    batch = cur.fetchmany(10000)
    if not batch:
        break
    rows += len(batch)
    if first:
        first = False
        print("FIRST_BATCH", flush=True)
        time.sleep(@PAUSE_S@)
print("DONE", rows, flush=True)
""" % (PORT, TOKEN)

IDLE_CLIENT_TEMPLATE = r"""
import time
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
@INSTALL@
con.execute("LOAD quack")
con.execute("LOAD httpfs")
con.execute("SET http_retries = 0")
con.execute("ATTACH 'quack:localhost:%d' AS rpc (token '%s', disable_ssl true)")
print("ATTACHED", flush=True)
time.sleep(3600)
""" % (PORT, TOKEN)

NEW_INSTALL = "con.execute(\"FORCE INSTALL quack FROM '{repo}'\"); con.execute(\"FORCE INSTALL httpfs FROM '{repo}'\")"
OLD_INSTALL = "con.execute(\"FORCE INSTALL '{ext}'\"); con.execute(\"FORCE INSTALL httpfs FROM '{repo}'\")"


def start_server(repo: str, lease_mode: str) -> duckdb.DuckDBPyConnection:
  server = duckdb.connect(config={"allow_unsigned_extensions": "true"})
  server.execute(f"FORCE INSTALL quack FROM '{repo}'")
  server.execute(f"FORCE INSTALL httpfs FROM '{repo}'")
  server.execute("LOAD quack")
  server.execute("LOAD httpfs")  # session-id generation needs the crypto module
  server.execute(f"SET GLOBAL quack_lease = '{lease_mode}'")
  # Enough rows that the first PREPARE batch (12 chunks) is nowhere near the end.
  server.execute("CREATE TABLE t AS SELECT range AS id FROM range(5000000)")
  server.execute(f"CALL quack_serve('quack:localhost:{PORT}', token := '{TOKEN}', disable_ssl := true)")
  return server


def stop_server(server: duckdb.DuckDBPyConnection) -> None:
  try:
    server.execute(f"CALL quack_stop('quack:localhost:{PORT}')")
  finally:
    server.close()


def spawn_client(script: str) -> subprocess.Popen:
  with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
    f.write(script)
    path = f.name
  return subprocess.Popen([sys.executable, path], stdout=subprocess.PIPE, text=True)


def wait_for_line(proc: subprocess.Popen, marker: str, timeout_s: float) -> str:
  deadline = time.monotonic() + timeout_s
  while time.monotonic() < deadline:
    line = proc.stdout.readline()
    if not line:
      raise AssertionError(f"client exited before printing {marker} (rc={proc.poll()})")
    if line.startswith(marker):
      return line.strip()
  raise AssertionError(f"timed out waiting for {marker}")


def connection_count(server: duckdb.DuckDBPyConnection) -> int:
  return server.execute("SELECT count(*) FROM quack_active_connections()").fetchone()[0]


def server_counter(server: duckdb.DuckDBPyConnection, key: str) -> int:
  return int(server.execute(f"SELECT info['{key}'] FROM quack_server_list()").fetchone()[0])


def new_client_script(pause_s: float) -> str:
  install = NEW_INSTALL.format(repo=os.path.abspath(REPO))
  return CLIENT_TEMPLATE.replace("@INSTALL@", install).replace("@PAUSE_S@", str(pause_s))


def probe_kill(mode: str) -> None:
  server = start_server(REPO, mode)
  try:
    client = spawn_client(new_client_script(pause_s=3600))
    wait_for_line(client, "FIRST_BATCH", 60)
    time.sleep(2)  # let the paused client's last FETCH settle server-side
    assert connection_count(server) == 1, "expected one registered connection before the kill"
    client.kill()
    client.wait()

    if mode == "enforce":
      deadline = time.monotonic() + REAP_DEADLINE_S
      while time.monotonic() < deadline:
        if connection_count(server) == 0:
          break
        time.sleep(2)
      else:
        raise AssertionError(f"connection not reaped within {REAP_DEADLINE_S}s")
      assert server_counter(server, "lease_expired_reaped") >= 1
      print(
        f"kill-enforce OK: suspended-result connection reaped, "
        f"detected={server_counter(server, 'lease_expired_detected')}"
      )
    else:
      time.sleep(REAP_DEADLINE_S)
      assert connection_count(server) == 1, "observe mode must not reap"
      assert server_counter(server, "lease_expired_detected") >= 1, "expiry not counted"
      assert server_counter(server, "lease_expired_reaped") == 0
      state = server.execute("SELECT lease_state FROM quack_active_connections()").fetchone()[0]
      assert state == "expired", f"lease_state={state}"
      print(
        f"kill-observe OK: entry retained, lease_state=expired, "
        f"detected={server_counter(server, 'lease_expired_detected')}"
      )
  finally:
    stop_server(server)


def probe_slow_consumer() -> None:
  pause = 3 * LEASE_S
  server = start_server(REPO, "enforce")
  try:
    client = spawn_client(new_client_script(pause_s=pause))
    wait_for_line(client, "FIRST_BATCH", 60)
    done = wait_for_line(client, "DONE", pause + 120)
    rows = int(done.split()[1])
    assert rows == 5000000, f"expected all rows, got {rows}"
    assert server_counter(server, "lease_expired_reaped") == 0, "healthy slow consumer was reaped"
    client.wait(timeout=30)
    print(f"slow-consumer OK: {rows} rows after a {pause}s mid-stream pause, nothing reaped")
  finally:
    stop_server(server)


def probe_sigstop() -> None:
  server = start_server(REPO, "enforce")
  try:
    client = spawn_client(new_client_script(pause_s=0))
    wait_for_line(client, "FIRST_BATCH", 60)
    os.kill(client.pid, signal.SIGSTOP)
    time.sleep(30)  # half the lease: heartbeats are stopped too, but the lease survives
    os.kill(client.pid, signal.SIGCONT)
    done = wait_for_line(client, "DONE", 120)
    rows = int(done.split()[1])
    assert rows == 5000000, f"expected all rows, got {rows}"
    assert server_counter(server, "lease_expired_reaped") == 0, "SIGSTOPped-but-alive client was reaped"
    client.wait(timeout=30)
    print(f"sigstop OK: {rows} rows after a 30s stop, nothing reaped")
  finally:
    stop_server(server)


def probe_old_client() -> None:
  old_ext = next(OLD_EXTENSION.glob("*/quack.duckdb_extension"), None)
  if old_ext is None:
    print(f"old-client SKIPPED: no cached v1.5-abandonment.1 artifact under {OLD_EXTENSION}")
    return
  server = start_server(REPO, "enforce")
  try:
    install = OLD_INSTALL.format(ext=old_ext, repo=os.path.abspath(REPO))
    script = IDLE_CLIENT_TEMPLATE.replace("@INSTALL@", install)
    client = spawn_client(script)
    wait_for_line(client, "ATTACHED", 60)
    version = server.execute("SELECT protocol_version FROM quack_active_connections()").fetchone()[0]
    assert version == 2, f"old client should negotiate v2, got {version}"
    time.sleep(REAP_DEADLINE_S)  # idle far past the lease; no heartbeats exist in v2
    assert connection_count(server) == 1, "a pre-heartbeat client must never be lease-reaped"
    state = server.execute("SELECT lease_state FROM quack_active_connections()").fetchone()[0]
    assert state == "n/a", f"lease_state={state}"
    client.kill()
    client.wait()
    print("old-client OK: v2 connection idle past the lease, never reaped, lease_state=n/a")
  finally:
    stop_server(server)


PROBES = {
  "kill-enforce": lambda: probe_kill("enforce"),
  "kill-observe": lambda: probe_kill("observe"),
  "slow-consumer": probe_slow_consumer,
  "sigstop": probe_sigstop,
  "old-client": probe_old_client,
}


def main() -> None:
  parser = argparse.ArgumentParser()
  parser.add_argument("scenario", nargs="?", choices=sorted(PROBES), help="run one scenario (default: all)")
  args = parser.parse_args()
  scenarios = [args.scenario] if args.scenario else list(PROBES)
  for name in scenarios:
    print(f"--- {name}")
    PROBES[name]()
  print("ALL PROBES PASSED")


if __name__ == "__main__":
  main()
