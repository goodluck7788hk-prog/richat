# Shyft Baseline and Hot-Path Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans for the critical path,
> with independent scoped audits and a fresh final review. The initial repair
> phase prohibited commits; the later source-publication authorization below
> permits commit/push only.

**Goal:** Start from the actual Shyft 4.3 Git baseline, add only necessary operator
repairs, and remove demonstrated avoidable forwarding-path overhead.

**Architecture:** A detached sibling worktree protects the original fork. Source
differences are classified before porting; correctness-preserving hot-path changes
are then evaluated using the retained regressions and a debug replay workload.

**Tech Stack:** Rust 1.97.1, exact Agave 4.3.2, Yellowstone proto 14.0.1, Tokio,
existing channel/storage test fixtures.

**Spec:** `docs/superpowers/specs/2026-10-23-shyft-baseline-design.md`.

## Global Constraints

All spec constraints apply. No release compilation, remote operation, deployment,
commit or push. Preserve strict history and original source-local bank semantics.

## Review Focus

- Single-source reconnect: replayed messages appear once, distinct original account
  write versions and transaction/entry payloads survive.
- Bank/source transitions: equal bank numbers from different validators never mix;
  parent invalidation and ambiguous finalization cannot erase required history.
- Disk reopen and retention: incomplete slots never masquerade as complete memory
  replay; source ownership survives and trimmed metadata cannot be resurrected.
- Slot-only traffic: no account/transaction capacity preallocation per candidate;
  finalization remains bounded and late footer handling stays correct.
- Defaults/provenance: no reconnect or new event option turns on implicitly, original
  worktree remains unchanged, and new HEAD is the pinned Shyft commit.

## Tasks

### Task 1: Baseline, inventory and compatibility

- [x] Verify the clean detached Shyft HEAD and inspect a debug baseline check.
- [x] Classify local repairs against Shyft in a checked-in-to-be comparison document.
- [x] Retain exact Agave/compiler/security compatibility, not unrelated dependency
  churn; remove duplicated conversion implementations already covered upstream.
- [x] Port necessary functional patches and regression tests by explicit file/hunk,
  leaving upstream-only changes intact unless a documented reason overrides them.

### Task 2: Demonstrate and remove avoidable hot-path work

- [x] Preserve the old-candidate implementation for a reproducible debug comparison.
- [x] Add a failing slot-only dedup-capacity guard before making allocation lazy.
- [x] Initialize/update slot ownership only on ownership transitions; avoid repeated
  per-message map lookup and Arc replacement without changing persisted ownership.
- [x] Keep bounded finalization cleanup; evaluate any additional cost found by audit
  rather than deleting correctness state or adding an unbounded background queue.
- [x] Run targeted channel/reconnect/storage tests after each implementation change.

### Task 3: Regression, measurement and handoff

- [x] Run all workspace tests, all-target debug checks, formatting and whitespace checks.
- [x] Check all five fuzz targets and audit both lockfiles without adding ignores.
- [x] Run repeatable local debug-only forwarding workloads and record limitations;
  do not claim production performance acceptance from development measurements.
- [x] Obtain a fresh independent review and address important findings with tests.
- [x] Record upstream overlap, retained patches, exact results and source provenance;
  verify the original source diff/status hashes are unchanged.

## Execution Ledger

- Approved by operator: independent local worktree; no publishing or online changes.
- Original HEAD: `058d518556680735a3b85554fb03f43e5a494198`.
- Original tracked diff SHA256: `4c255aabfc5db3728de714a2019e879ebcd38b97b89ae285fd156d86a4faaa9b`.
- Original porcelain-status SHA256: `cec951e751aaefbb786477453f6f123c787d529695b1237a226ad7927b8a39c7`.
- Native worktree tool returned `Not a git repository` for the `.slv` task. Git
  fallback creates the approved detached sibling worktree; no new branch is made.

## Results

- Pristine Shyft debug workspace check passed before any source patch.
- First ported candidate reproduced the prior 138 passing regressions before
  performance work. Final workspace suite: **157 passed, zero failed, one ignored
  debug timing test**; all doc-tests passed. The timing test was run explicitly
  before and after optimization and passed both times.
- All-target workspace debug check, all five fuzz-bin checks, formatting and
  whitespace checks passed. Stable rustfmt reports only the repository's existing
  nightly-only import-option warnings.
- Root and final fuzz locks audit to **zero known vulnerabilities** against
  RustSec database commit `f7b2f6bc663452200bbe639ebb75e0c762b3422b`. Existing
  bincode/rustls-pemfile/unsafe-libyaml unmaintained and metrics 0.24.5 yanked
  warnings remain. Lock reconciliation only removed unused direct dependency
  edges; reviewed registry package versions and checksums are unchanged.
- Shyft's V1 encoders/convert_to and original benchmark fixtures remain intact;
  redundant status converters and the old DeactivatedStake=Staking mapping were
  not imported. The comparison document distinguishes upstream features from
  required local fixes and intentional replay/availability policies.
- Independent review found the sparse-first-entry lazy-vector regression, which
  was reproduced and repaired with checked index growth and boundary tests.
  Final scoped recheck reports no remaining actionable findings; it does not
  establish production performance acceptance.
- Original tracked-diff and status hashes match their initial values. New HEAD
  remains the exact Shyft commit, detached; no commit, branch, GitHub fork, push,
  release artifact, remote operation, deployment or service change was made.
- See `docs/SHYFT-43-PERFORMANCE.md` for development timings and explicit limits.
  No production throughput, sustained RSS or end-to-end P99 claim is made.

## Subsequent source publication authorization

After approving independent-source promotion, the operator authorized committing
and pushing the new official source and reviewed repairs to the existing fork.
Use its existing local `agave-4.3` branch name, preserving Shyft ancestry and the
old checkout. Include only sanitized SLV source/preflight snapshots and their
portable local regression harness, never the private controller workspace.
Publish without tags or a PR so the inherited release workflows are not selected.
Refresh local debug tests and source contracts before committing. This later
authorization does not permit release compilation, deployment, switching,
production access or service changes.
