# Per-subscription gRPC backlog implementation plan

**Goal:** expose source-IP and subscription-specific pending bytes and reader lag
for the Dragon's Mouth `subscribe` and `subscribe_accounts` streams.

**Architecture:** sample existing queue accounting and cursor state only when
Prometheus scrapes. A response-owned registration identifies each active stream
by transport peer IP, server-generated subscription ID and optional client name.
Do not add metric lookups, formatting, locks or atomics to normal message delivery.

## Constraints and decisions

- Start in the clean official `agave-4.3` checkout at `6b90e5c`; implementation
  does not create a branch, publish, deploy, access production or restart it.
  The user separately authorized source commit/push after validation; rollout
  authorization and runtime reconnect configuration are separate gates.
- The user explicitly permits local debug tests, not release compilation.
- Reuse the historical debug artifact cache through `CARGO_TARGET_DIR` only;
  never select the archived checkout as a source.
- Trust only `Request::remote_addr()`, not forwarded/client-supplied IP headers.
- Blank or duplicate client names must still produce distinct subscriptions.
- Build escaped labels once at registration; export active streams directly
  rather than permanently registering per-reconnect metric handles.
- Snapshot client state with `try_lock`, release it before formatting, and mark
  unavailable cursor observations explicitly rather than reporting false zeroes.
- Pending bytes cover encoded payloads in Richat's application queue, not HTTP/2,
  TCP, or the receiving application's own queue.
- Memory lag counts unfiltered channel records at the subscription's commitment,
  not matched outputs, elapsed time or client-processed transactions.
- Disk replay uses a different cursor domain and must never be subtracted from a
  memory-channel cursor. Resolve its observation semantics separately, without
  changing replay delivery or adding per-record work.
- Fix terminal error draining to subtract dropped payload lengths from the
  existing accounting, rather than resetting an atomic underneath a consumer.

## Task 1: accounting regression

- [x] Add a real queue-drain regression and observe its failure.
- [x] Repair cold-path terminal draining, including repeated errors and a
  concurrently dequeued payload. Preserve the normal push/poll paths.

## Task 2: scrape-only subscription registry

- [x] Add tests for unique identity, transport peer attribution, label escaping,
  bounded client labels, unknown peer and cleanup on response drop/error.
- [x] Implement a response-owned registry entry with weak queue/state references.
- [x] Render pending bytes, cursor lag and observation/replay availability only
  for currently active streams. No stale per-subscription recorder series.
- [x] Verify scrapes never wait for a busy client state mutex and do not retain
  subscription resources after response cancellation.

## Task 3: cursor sampling and integration

- [x] Test zero/unset/busy cursors, commitment-specific lag, eviction, cursor
  advancement, disk replay and the disk-to-memory transition.
- [x] Wire the shared registry into the daemon metrics callback and both gRPC
  subscribe methods. Preserve existing aggregate metrics and readiness checks.
- [x] Protect hot-path source structure against adding metric registration,
  formatting or registry access to push/poll/worker delivery loops.

## Task 4: validation and documentation

- [x] Run targeted regressions, then the local debug workspace suite.
- [x] Run formatting and source-contract checks; confirm unchanged lockfiles.
- [x] Obtain an independent review of locking, lifecycle, labels, cursor domains
  and normal forwarding overhead; address material findings.
- [x] Document metric units, unknown values, PromQL, direct-peer/proxy caveats,
  scrape cost and the explicit absence of production performance evidence.

## Progress

- Initial source tree is clean; no production access is used by this task.
- Local debug-test compilation was explicitly approved in this conversation.
- Accounting regressions failed before repair: 42 bytes remained instead of zero,
  and 42 remained instead of the 19 bytes owned by an in-flight dequeue.
- 25 new regressions are included; the targeted gRPC group passes all 30 tests.
- `cargo test --locked --workspace`: 182 passed, zero failed, one intentionally
  ignored debug performance workload. Log: `/tmp/richat-backlog-workspace.log`.
- `cargo test --locked --workspace --all-targets --no-run` and formatting pass.
- A comparison with HEAD verifies the filter worker, normal queue push and normal
  response poll are byte-for-byte unchanged. Channel/storage/producer sources and
  dependency lockfiles are unchanged.
- Ruling: disk replay lag stays explicitly unknown, with a separate replay flag
  and reason. Its cursor has different units/bootstrap semantics, so guessing a
  numeric lag would give false confidence. No replay behavior is modified.
- Independent design review agrees with the unavailable-state contract and
  identified the need to aggregate unavailable subscriptions alongside known lag;
  the documentation includes both queries.
- Final review found one P2: metric metadata/samples were not grouped by family.
  A two-subscription exposition regression reproduced it; snapshot-once,
  family-oriented output now passes. The reviewer accepted the final patch.
- A real localhost TCP regression covers reused HTTP/2 client-channel identity,
  request half-close, response-drop cleanup and graceful cancellation. TLS peer
  propagation was source-reviewed but does not have a new certificate-based test.
- Implementation checks were local only. No release build, production access,
  deployment, restart, commit or push occurred during that implementation step.
- Publication was separately authorized on 2026-10-25. The full debug workspace
  suite and formatting were rerun before committing; the source is published to
  the operator fork's `agave-4.3`, not to the upstream `origin` remote.
