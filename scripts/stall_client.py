#!/usr/bin/env python3
"""Client that starts draining a large streaming result, then stops reading.

Run as a separate process so SIGSTOP freezes a real socket peer mid-fetch, leaving the
server parked writing frames to a connection nobody is reading. A thread in that state
holds `QuackConnection::lock` but is NOT inside DuckDB execution, so `Interrupt()` has
nothing to interrupt — the hypothesis for galwey/prod's stuck-in-'cancelling'.

Prints STALL_CLIENT_FETCHING once frames are actually flowing, so the parent knows the
server has a request parked before it freezes us.

Usage: stall_client.py <port> <token> <repo> <sql>
"""

import sys

import duckdb


def main() -> int:
  port, token, repo, sql = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
  con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
  con.execute(f"FORCE INSTALL httpfs FROM '{repo}'")
  con.execute(f"FORCE INSTALL quack FROM '{repo}'")
  con.execute("LOAD httpfs; LOAD quack")
  con.execute("SET http_retries = 0")

  con.execute(f"SELECT * FROM quack_query('quack:localhost:{port}', '{sql}', token := '{token}', disable_ssl := true)")
  # Pull one chunk so the stream is genuinely in flight and the server is mid-send.
  reader = con.fetch_record_batch(1024)
  next(reader)
  print("STALL_CLIENT_FETCHING", flush=True)

  # Keep the result open and stop consuming. The parent SIGSTOPs us here.
  while True:
    next(reader)


if __name__ == "__main__":
  sys.exit(main())
