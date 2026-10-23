# Official operator Richat source

This independent checkout is the official maintenance and staged-build source as
of 2026-10-23. Its Git baseline is Shyft
`3a14a3d69f3d0b445bbb30c99cd40c001dd2b910`, with reviewed local repairs on top.
The old `../richat` checkout and dated worktree backup are archives, not sources
to edit, automatically merge or deploy.

## Compatibility and invariants

- Target exact Agave 4.3.2, Rust 1.97.1 and Yellowstone proto 14.0.1. Preserve both
  root and fuzz lock security fixes; do not copy another fork's locks wholesale.
- Plugin `.so` and daemon are distinct components sharing the wire schema. Any
  later deployment upgrades the consumer before the producer.
- Bank IDs are source-local. Preserve ownership fences, cleared-bank handling,
  original single-source account versions and complete replay identities.
- Missing history is an explicit replay failure, never a silent jump to live.
  Keep bounded finalized state, durable source provenance and incomplete-memory
  replay routing. New notifications and idle timeout remain opt-in.

## Validation and release boundary

- Read `docs/SHYFT-43-REPAIR-AUDIT.md` and `docs/SHYFT-43-PERFORMANCE.md` before
  changing protocol/recovery behavior. Add regressions for demonstrated defects.
- Use local `cargo test --locked --workspace`, all-target debug checks, formatting
  checks and fuzz-bin checks. Debug timing is not production performance proof.
- `~/.slv/scripts/build-richat-staged.sh` now defaults to this directory. It rsyncs
  source including `.git`; keep a self-contained ordinary Git checkout, with no
  linked-worktree pointer, metadata symlink or external objects alternates.
- Maintain the `agave-4.3` branch in the operator fork; keep the dirty-tree guard.
  Commit/push authorization does not authorize `--allow-dirty`, release builds,
  production access, deployment, restarts or switching. Each needs explicit
  user authorization. Do not push tags or create a PR as part of source-only
  publication; upstream release workflows match `master`, `agave-v*` and tags.
- `ops/slv/` contains shareable integration snapshots and local-only tests, not
  production inventories. Keep controller secrets and host-specific data out.
- Source promotion does not change production 4.3.0 pins or enable reconnection
  online. Release labels must match the exact 4.3.2 interface when a build is
  explicitly authorized. Use `~/.slv/UPGRADE.md` for that separately gated flow.
- The active historical tag-clone installers reject Agave 4.3 before cloning.
  Dedicated fanout rollout needs its own reviewed local-source flow; do not
  remove that guard or assume a published archive tag contains these repairs.
