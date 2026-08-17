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

1. **SQL logic tests** — the full suite, including cancellation/negotiation,
   socket-liveness and lease setting coverage:

   ```sh
   make test
   ```

2. **Cancel/complete race stress** — 1,000 iterations racing operator cancels
   and client interrupts against completion, with socket liveness AND the lease
   in enforce; fails on any hang, unexpected error, or leaked registry entry:

   ```sh
   python3 scripts/stress_cancel.py 1000
   ```

3. **Lease probes** — real server + real client subprocesses covering
   kill-between-fetches (enforce reaps ≤75s / observe counts only — the Canyon
   leak), a slow consumer pausing 3× the lease (heartbeats keep it alive),
   SIGSTOP false-positive guard, and an old v2 client that must never be
   lease-reaped:

   ```sh
   python3 scripts/probe_lease.py            # all (~7 min)
   python3 scripts/probe_lease.py sigstop    # one scenario
   ```

4. **Layer 1/2 behavior probes** — real server + client subprocesses covering
   SIGKILL mid-query, local interrupt, operator cancel, and iptables-silenced
   partition (keepalive). The ad-hoc harness and results to date are recorded
   in `docs/plans/duckdb-quack-phase0-findings.md` (mono). Re-run at minimum the
   SIGKILL-mid-CTAS enforce probe when touching the socket-liveness or
   cancellation paths.

5. **Caveat when testing by hand:** `INSTALL quack FROM <repo>` silently keeps a
   previously cached extension — always `FORCE INSTALL quack FROM
   '<...>/build/release/repository'` or you are validating a stale binary. Also
   `LOAD httpfs` on the *server* connection: session-id generation needs its
   crypto module, and without it every CONNECTION_REQUEST 500s.

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
the new tag, per-platform SHA-256 values, and `extension_version` (the fork commit short sha, stamped into the build and enforced everywhere); the image builds, test helpers, and
benchmark tooling all read that one file.

## Layout notes

- Protocol messages: `src/include/quack_message.hpp` (+ `quack_message.json`
  spec; `serialize_quack_message.cpp` follows DuckDB's serializer conventions).
- Connection lifecycle/state machine: `src/quack_server.cpp` — read the
  `state_lock` comment in `quack_server.hpp` before touching locking.
- Socket liveness: `src/quack_socket_watch.cpp`; modes via the
  `quack_socket_liveness` setting (`off`/`observe`/`enforce`, default observe).
- Lease/heartbeat (Layer 3): server sweep in `src/quack_lease_reaper.cpp`
  (60s lease, 15s sweep), client heartbeat thread in
  `QuackClientConnection::HeartbeatLoop` (`src/quack_client.cpp`, ~20s); modes
  via the `quack_lease` setting (`off`/`observe`/`enforce`, default observe).
  Both watcher settings are read from background threads with no session, so
  only `SET GLOBAL` is visible to them.
- Client interrupt path: `HttpsQuackClient::RequestInternal` in
  `src/quack_client.cpp`.
- Protocol versions: v2 = targeted CANCEL_REQUEST, v3 = HEARTBEAT + lease
  reaping. `MAX_QUACK_VERSION` in `src/include/quack_server.hpp` is the single
  constant both sides negotiate on; lease reaping is gated on ">= 3" so a
  server only ever reaps clients that are able to heartbeat.
