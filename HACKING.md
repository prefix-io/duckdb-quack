# Hacking on the Prefix quack fork

This fork (`prefix-io/duckdb-quack`) hardens query abandonment/cancellation on
branch `prefix/v1.5-abandonment` (production, builds against DuckDB v1.5.5) with
`prefix/main-abandonment` as the experimental mainline reference. Design and
validation history: `docs/plans/duckdb-quack-cancellation-hardening.md` and
`docs/plans/duckdb-quack-phase0-findings.md` in the mono repo.

There is deliberately **no CI** here — the fork changes rarely. Instead, run the
checklist below by hand for every change, in order. All of it runs on a dev
machine.

## Build

```sh
sudo apt-get install -y cmake ninja-build g++ libssl-dev libcurl4-openssl-dev
git submodule update --init          # duckdb pinned at v1.5.5 (d8cdaa33fd)
GEN=ninja make release               # ~15 min cold, seconds warm
```

Output: `build/release/extension/quack/quack.duckdb_extension` plus a
DuckDB-style extension repository under `build/release/repository/`.

## Validation checklist (run all of these before publishing)

1. **SQL logic tests** — the full suite, including cancellation/negotiation and
   socket-liveness setting coverage:

   ```sh
   make test
   ```

2. **Cancel/complete race stress** — 1,000 iterations racing operator cancels
   and client interrupts against completion; fails on any hang, unexpected
   error, or leaked registry entry:

   ```sh
   python3 scripts/stress_cancel.py 1000
   ```

3. **Behavior probes** — real server + client subprocesses covering SIGKILL
   mid-query, kill-between-fetches, local interrupt, operator cancel, SIGSTOP
   (false-positive guard), and iptables-silenced partition (keepalive). The
   probe harness lives in the mono working notes; results to date are recorded
   in `docs/plans/duckdb-quack-phase0-findings.md` (mono). Re-run at minimum the
   SIGKILL-mid-CTAS enforce probe and the SIGSTOP probe when touching the
   socket-liveness or cancellation paths.

4. **Caveat when testing by hand:** `INSTALL quack FROM <repo>` silently keeps a
   previously cached extension — always `FORCE INSTALL quack FROM
   '<...>/build/release/repository'` or you are validating a stale binary.

## Releasing artifacts for mono

Mono consumes pinned, checksum-verified release assets (never `FROM core`, never
"latest"). To publish:

```sh
git push                                        # artifacts build from the pushed commit
scripts/release.sh --publish v1.5-abandonment.N # builds linux amd64 + arm64, uploads
```

`scripts/release.sh` builds each platform in a clean container from the pushed
commit (`scripts/artifact.Dockerfile`), writes `out/SHA256SUMS`, and creates the
GitHub release. amd64 builds run emulated on arm64 dev machines — slow (an hour
or more) but correct.

Then update `prefix/common/data_warehouse/duckdb/quack_pin.json` in mono with
the new tag and per-platform SHA-256 values; the image builds, test helpers, and
benchmark tooling all read that one file.

## Layout notes

- Protocol messages: `src/include/quack_message.hpp` (+ `quack_message.json`
  spec; `serialize_quack_message.cpp` follows DuckDB's serializer conventions).
- Connection lifecycle/state machine: `src/quack_server.cpp` — read the
  `state_lock` comment in `quack_server.hpp` before touching locking.
- Socket liveness: `src/quack_socket_watch.cpp`; modes via the
  `quack_socket_liveness` setting (`off`/`observe`/`enforce`, default observe).
- Client interrupt path: `HttpsQuackClient::RequestInternal` in
  `src/quack_client.cpp`.
