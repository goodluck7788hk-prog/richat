# Local forwarding-path checks

## What changed

- Slot ownership is initialized on acquisition, including an initial unbanked
  notification followed by bank-aware data. Established ownership is not looked
  up and replaced with another Arc clone for each accepted message.
- The storage owner is attached to the first owned journal record only. Its
  `source_persisted` flag changes only after checked queue submission. Failure to
  enqueue is explicit, not a false promise that ownership has been saved.
- Collector metadata coalesces repeated identical ownership without discarding
  conflicting-owner evidence, resetting the original replay head or recreating a
  trimmed slot. Ordinary records with no metadata take no metadata lookup path.
- Account/transaction dedup and entry/message capacities are allocated only when
  that payload type actually appears. Actual payloads retain their previous
  reserve sizes to avoid repeatedly growing a table in the common populated case.
- Inline SmallVec storage handles up to 16 registered wakers without a heap
  allocation. Empty wake lists are not detached, and all wake calls remain outside
  channel/replay locks. More than 16 waiters may still allocate.
- Finalized candidate payload capacities are still freed. Compact history for late
  footers must not retain large account/message buffers just to avoid a deallocation.

## Regression evidence

Before implementation, three lazy-capacity guards failed with the eager table
capacity 14,336, and both producer/daemon pending-wake-cycle guards failed after
the old Vec capacity was detached. After implementation those tests pass.
Coverage includes 0/1/16/17 waiters, entry index zero, single-source account-only
traffic, late ownership acquisition, canonical-bank isolation and retained-history
capacity. Real disk reopen tests cover provenance, original replay indices and
single-source dedup after ownership is acquired on a recovered unbanked prefix.

Independent review caught an initial lazy-vector boundary regression: a first
entry at index 256 could resize to exactly 256 and then index out of bounds.
Regression tests reproduced that failure and an overflowing index. Growth now
uses checked `index + 1` before rounding; sparse first indices 255/256/257,
511/512 and 1,023/1,024 are covered, including repeated delivery.

## Reproducible development workload

Run with the installed Rust 1.97.1 toolchain, without `--release`:

```sh
cargo test --locked -p richat forwarding_replay_profile --lib -- \
  --ignored --nocapture --test-threads=1
```

The test constructs/parses inputs outside the timing window, performs five warmup
iterations and twenty measured iterations, and checks accepted message cardinality
and finalized progress outside timing. It reports nanoseconds per input, including
duplicate inputs which do not become outputs. It is deliberately ignored by the
normal test suite and has no unstable wall-clock pass/fail threshold.

Observed on the local Apple Silicon development machine:

| Artificial scenario | Before median ns/input | After median ns/input |
| --- | ---: | ---: |
| 256 slot-only create/finalize pairs | 11,688.1 | 10,402.6 |
| 8,192 unique account updates in one bank | 2,899.5 | 2,827.8 |
| Replay of the same 8,192 account updates | 1,236.1 | 1,240.2 |

Logs: `/tmp/richat-shyft43-profile-before.log` and
`/tmp/richat-shyft43-profile-after.log`. The before candidate reused the reviewed
repairs, before these hot-path changes; it is not pristine Shyft. Source fingerprints
before optimization: daemon channel SHA256
`7dff2485f5309cdf70d6ad212c4abfa40f154a099c13dbfa9c2776e57bde82e3`,
producer channel SHA256
`5031cd950fd7bef28637211218b927a034e5766faa6d5a288076b0849314012e`.

These are local debug observations, not controlled production benchmarks. Account
and duplicate sample ranges overlap; the small difference is not evidence of a
statistically meaningful speedup or regression. These scenarios do not measure
Linux Zen5 release code, transaction-heavy/full-block throughput, disk throughput,
allocator contention, networking, sustained RSS or end-to-end P99 latency.

## What is not performance-free

Lossless reconnect requires replay identity state and hashing. Correct bank
semantics require bank/source selection. Bounded storage can apply backpressure,
and finalization must dispose of retained buffers. These necessary costs have not
been removed or disguised by dropping records, skipping history or buffering
without a bound. Production performance acceptance remains a later user-controlled
release-window task; no release build or node operation occurred here.
