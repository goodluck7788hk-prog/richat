# Shyft 4.3 and operator repair audit

## Provenance and scope

Baseline: Shyft `feat/agave-4.3.0-upgrade`, commit
`3a14a3d69f3d0b445bbb30c99cd40c001dd2b910` (branch head verified 2026-10-23).
The official maintenance branch `agave-4.3` starts at that commit. The original operator checkout at
`058d518556680735a3b85554fb03f43e5a494198`, including its uncommitted preparation,
is preserved. Reviewed patches here retain the Shyft commit as their Git ancestor.

The operator promoted this source on 2026-10-23. The directory now contains a
self-contained independent Git clone rather than a linked `.git` pointer, and
staged maintenance/build defaults select it. Promotion does not publish or deploy
the code; see `SHYFT-43-OFFICIAL-SOURCE.md`.

The pristine Shyft workspace passed a local `cargo check --locked --workspace`
with Rust 1.97.1. This proves a debug check only, not plugin ABI compatibility
with a particular node, correct replay semantics or production performance.

## What upstream already has, and what still needs a patch

References below refer to the pinned upstream commit, not moving line numbers.

| Area | Shyft state | Decision |
| --- | --- | --- |
| V1 transaction raw/prost encoding and basic decoding | Already implemented in `plugin-agave/src/protobuf/encoding/transaction.rs` and `proto/src/convert_{to,from}.rs` | Reuse upstream implementation; do not port duplicate `create_status_*` aliases/converters. Retain malformed-input checks and real transaction/block regressions. |
| Reward enums, including DeactivatedStake=5 and VATDebit=6 | Correct in upstream raw and Prost encoders | Drop the old operator mapping of DeactivatedStake to Staking=3; add parity/round-trip coverage for every reward enum. |
| Bank-aware callbacks, wire fields, footer/parent notifications and filters | Substantive 4.3 support already present | Reuse it, retaining opt-in defaults. Patch actual omissions instead of reimplementing all callbacks. |
| Limited parsing and sliced accounts | Slot/entry bank accessors are incomplete; sliced account startup uses `!is_startup` | Retain bank extraction, UTF-8/range validation and startup fidelity regressions. Raw forwarding is not universally broken upstream. |
| Live cursor, synchronous tail and wait registration | Plugin subscribes at `tail`; synchronous daemon read uses `< tail`; daemon registration lacks publication recheck | Retain coordinated cursor/reader fixes. Plugin's existing locked registration recheck is reused. |
| Single-source reconnect and canonical accounts | Dedup gate excludes one reconnecting source; multi-source path rewrites versions; old/equal canonical writes can survive | Retain distinct single-source dedup and canonical-buffer repair, preserving accepted Processed updates and original versions. |
| Replay failure, backoff and idleness | Strict unavailable-history failure already exists; retry resets at handshake; stale failure votes persist | Reuse strict failure; retain checkpoint-scoped classification, data-driven backoff and optional idle timeout. Never import a silent jump-to-live fallback. |
| Bank assembly, invalidation and ownership | Assembly/dedup is slot-based, parent marker does not clear bank state | Retain separate bank candidates, source-local ownership, invalidation/commit fences and bank/version-aware replay IDs. |
| Durable replay and late footer | No persisted owner or complete-memory-slot marker; finalized slots reject all late events | Retain bounded late footer delivery, durable owner provenance, original replay heads, partial-memory routing and anti-resurrection checks. |
| Blockhash cache and unary commitments | Expiry retain predicate is reversed and last-valid boundary is excluded; metadata is not candidate-bank aware | Retain expiry/boundary and canonical-bank repairs. No claim that cache growth caused the observed production incidents. |
| gRPC/WebSocket task lifetimes and small cache indices | Existing response-drop flag is partial; request reader can remain blocked; socket lifetime is not HTTP lifetime; cache subtraction can underflow | Retain event-driven cancellation, socket ownership and saturating arithmetic. |
| Producer/daemon lock scope | Wake and bulk payload destruction can occur under channel locks | Retain unlock-before-wake/drop; optimize detached waker storage separately. |
| Agave/compiler/security dependencies | Branch is the 4.3 line, not the operator's exact audited 4.3.2 set | Retain exact 4.3.2/Rust 1.97.1/proto 14.0.1 and both security locks. Preserve upstream dependency cleanup where compatible. |

## Deliberate compatibility and availability policies

These are not claims of behavior identical to Shyft:

- EntryUpdateParent is a Processed control event used immediately for invalidation;
  it is not replayed into Confirmed/Finalized as if it were canonical bank data.
- A bank-aware slot belongs to its first upstream source. Same numeric bank IDs
  from different validators cannot be mixed. This is not mid-slot failover;
  unresolved ownership/finality fails explicitly rather than discarding history.
- An unfinished legacy banked journal without source provenance can refuse startup.
  Guessing its owner would make replay integrity unverifiable. Do not delete or
  reinterpret historical journals automatically.
- Explicit native replay refuses an incomplete recovered in-memory prefix;
  standard gRPC can use the retained complete disk history instead.

## Hot-path work identified for this rebaseline

- Set slot ownership on acquisition only, not with a lookup and Arc replacement
  for each accepted message; ownership can appear after an initial unbanked status.
- Persist ownership on its first ordered owned journal record only, not every
  record. Preserve per-slot initialization and writer-side owner conflict checks.
- Lazily reserve payload-specific dedup and assembly capacities. Slot-only banks
  must not eagerly allocate 8,192-account/transaction and 16,384-message buffers.
- Detach inline waker storage before waking, without losing reusable small-waiter
  capacity or reintroducing lock-held/reentrant wakeups.
- Keep finalized candidate buffers released while compact history is retained for
  late footers. Removing `shrink_to_fit` alone is not an acceptable optimization.

## Acceptance boundary

Functional tests, structural capacity guards, local debug comparisons and an
independent review are required. Development measurements do not prove Linux
release throughput, P99 latency or allocator behavior. No release compilation,
deployment, service change or online configuration is authorized by this work.
