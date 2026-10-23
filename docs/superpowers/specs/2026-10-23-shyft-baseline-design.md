# Shyft 4.3 baseline and operator repairs

## Approved intent

The operator approved an independent local worktree rooted at Shyft commit
`3a14a3d69f3d0b445bbb30c99cd40c001dd2b910`, retaining the original checkout and
all its uncommitted preparation. Use Shyft's 4.3 functionality as the actual Git
baseline, not merely a description of source cherry-picking into the old fork.

## Constraints

- Local debug/test/check work only. No release build, SSH, deployment, service
  change, switch, GitHub repository creation, commit, branch creation or push.
- Keep exact Agave 4.3.2, Rust 1.97.1 and Yellowstone proto 14.0.1 compatibility;
  retain both reviewed security lockfiles unless a necessary, audited update is
  independently justified. Baseline ownership does not justify unsafe dependencies.
- Original checkout `~/dev/claude-project/richat` stays untouched.
- Strict replay is retained: missing history is an explicit error, never a silent
  live jump. Source-local bank identity, ownership fences, bank invalidation and
  replay completeness must not be weakened for performance.
- New notifications and idle timeout remain disabled by default. Reconnect policy
  is not activated online by changing this local source.

## Approach and rationale

Inventory every difference from Shyft before porting. Retain Shyft's existing V1,
bank-aware callback and new notification implementations where semantically sound;
do not preserve duplicate local implementations merely because they existed.
Retain independently demonstrated reader, recovery, lifecycle, validity and data
integrity repairs. Document each category as upstream-complete, partial or absent,
with source evidence and regression coverage.

Remove avoidable per-message ownership lookups/refcount churn. Avoid large eager
dedup allocations for slot-only events while preserving the actual replay keys and
single-source write versions. Finalization must still release bulk buffers; moving
or deleting that work is acceptable only with bounded-memory tests. Do not trade
correctness or unlimited buffering for a faster microbenchmark.

Use behavioral tests, structural allocation/capacity guards where appropriate and
a reproducible local debug-only replay workload. Report measurements as local
development evidence, not proof of production Linux throughput or P99 latency.

## Initial isolation and subsequent source promotion

The native worktree tool cannot operate on the task's non-Git `.slv` directory;
the approved Git fallback creates a detached sibling worktree. No original source
or deployment default is changed. The existing staged builder rsyncs `.git` and
runs Git remotely: this linked worktree's `.git` is a pointer to the controller,
so the initial worktree was NOT a usable build source without an ordinary
checkout/export preserving Git provenance.

The operator subsequently authorized official-source promotion on 2026-10-23.
The original linked worktree was moved to a dated backup, and the same formal
path now contains an independent ordinary Git clone with all 155 source files,
contents, modes and symlinks verified identical before documentation updates.
The build default and maintenance instructions now select the independent clone;
release compilation, commits, publishing and deployment remain separately gated.
