# Cancellation repro harnesses (galwey/prod 2026-09-14)

## The incident

An ad-hoc read on galwey/prod (`SELECT` over `actions_d.inbox_automatic_write_source`,
a `BLOCKWISE_NL_JOIN`) ran for **3 hours** and could not be killed. All three reap
paths fired and none landed:

| Layer | Evidence | Fired | Stopped it |
|---|---|---|---|
| operator `quack_cancel_connection` | HTTP 200 at 19:15:12 | yes | **no** |
| socket liveness | `socket_liveness_cancelled: 1` | yes | **no** |
| lease expiry | `lease_expired_reaped: 1` | yes | **no** |

The query sat in `cancelling` for 6,720s and only cleared on a warehouse restart.

## NOT reproduced

These scripts **do not** reproduce it. That is the finding, not a failure to try —
each one holds the cancel constant and varies one axis away from `stress_cancel.py`,
which passes and therefore cannot catch this. Eight conditions were eliminated:

enforcement modes, concurrency, spilling, late cancel (settled vs. 0-15ms),
result streaming, a non-draining client, a *frozen* client (SIGSTOP mid-fetch,
a real socket peer), and production topology.

Do not re-run these expecting a hang. Run them to confirm an axis is still clean
after a fork change.

## What killed the black-box theories

A gdb dump of the wedged server showed **0 of 172 threads running**. Four successive
C++ hypotheses — stale query identity, a swallowed `catch (...)`, a missing
`bound_connection`, a parked socket write — all died on that one observation at once.
Prefer the stack dump to another round of reasoning from counters.

`cancel_requests_received` is **not** a witness for operator cancels: measured
`before=0 after=0` across a successful `quack_cancel_connection`. It does not track
that path at all. Reading it as a smoking gun cost two wrong diagnoses.

## The scripts

- `stress_cancel_streaming.py` — six scenarios varying one axis each. **A skipped
  scenario returns `None` and the run reports INCONCLUSIVE, never PASS.** The first
  version reported PASS on scenarios that never ran, which is the exact
  "silence is not success" trap this whole investigation kept falling into.
- `repro_galwey.py` — the closest stage: a real client process frozen with SIGSTOP
  mid-fetch, so the server is genuinely parked writing frames to a peer that will
  never read. In-process harnesses miss this because they either drain or never fetch.
- `stall_client.py` — the child process for the above; prints `STALL_CLIENT_FETCHING`
  once frames are actually flowing, so the parent freezes it at the right moment.
- `wedge_and_dump.py` — wedges via the client-interrupt path and holds it open for an
  **external** debugger. gdb must attach from outside: `gdb -p $(getpid)` on yourself
  stops the thread waiting for gdb and deadlocks.

## Upstream

The fork is **ahead** of upstream on cancellation. Upstream's `655d672b4` would be a
regression here — it takes `conn->lock` where we take `state_lock`. Do not cherry-pick
it. Related: PRFX-3836 (racy interrupt in `probe_stream_abandonment`).
