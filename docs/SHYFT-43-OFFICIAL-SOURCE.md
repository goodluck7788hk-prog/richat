# Official maintenance and build source

## Operator decision, 2026-10-23

The authoritative directory is `~/dev/claude-project/richat-shyft-4.3` for all
subsequent local maintenance and staged plugin/daemon builds. Its `agave-4.3`
maintenance branch starts at Shyft
`3a14a3d69f3d0b445bbb30c99cd40c001dd2b910` plus the reviewed repairs, exact
Agave 4.3.2, Rust 1.97.1 and Yellowstone proto 14.0.1.

The old `~/dev/claude-project/richat` is preserved unchanged. The initial linked
worktree is preserved at
`~/dev/claude-project/richat-shyft-4.3-worktree-backup-20261023`.
Neither archive is a maintenance or default build source.

## Why an independent checkout was required

The staged builder rsyncs `.git` and runs Git inside the remote source directory.
A linked worktree has only a `.git` pointer into the controller's old repository;
copying that pointer cannot create a remotely usable checkout. Promotion moved
the linked worktree aside using Git, created a local independent clone without
object hardlinks/alternates, checked out the pinned Shyft baseline, then copied
tracked and untracked source files without altering their contents or modes.

All 155 pre-promotion files were verified against a path/mode/symlink/content
manifest before documentation updates. Manifest SHA256:
`cd7b3194658100d5ec5d40b3553a88a724d2a6647916fd467f869e7bfe906f7b`.
No commit, new named branch, GitHub repository or remote push was created
during that promotion phase.

The operator subsequently authorized committing and pushing this source and its
repairs to the existing operator fork, using the existing local `agave-4.3`
branch name. No existing remote branch is rewritten. The archived old source
remains unchanged. Publication pushes only this branch, with no tags or PR;
it does not trigger the upstream release workflows, whose push branches are
`master` and `agave-v*`. Release compilation, deployment and switching remain
explicitly prohibited by this authorization.

The sanitized `ops/slv/` bundle versions the relevant controller build/preflight
files and local-only regression harness. It intentionally excludes inventories,
credentials, live service configuration and the private controller workspace.

The active staged playbook and wrapper documentation select this directory.
Controller provenance inspection now rejects linked-worktree pointers, any Git
metadata symlinks, external common directories/object alternates, Git-context
environment overrides and unavailable HEAD commit objects
before source synchronization or build actions. Explicit `--src` remains available
for an independently reviewed ordinary checkout; it does not bypass these checks.

The active historical `install_richat.yml` and `geyser_richat_build.yml` tag-clone
routes reject Agave 4.3 labels before cloning/building. They cannot silently
replace this source with an archive tag. Dedicated fanout installation remains a
separately reviewed rollout task rather than an excuse to bypass that guard.

## Fixed versus intentionally pending

The confirmed cursor/wakeup, reconnect/dedup, bank semantics, durable recovery,
cache/lifetime and avoidable hot-path defects are repaired and regression-tested.
Shyft's already-correct V1/reward encoders are retained instead of duplicate old
patches. See the full repair audit and performance record for exact coverage.

These statements do not mean every possible defect is excluded:

- Production Linux release throughput, P99 latency, sustained RSS and long-run
  stability have not been accepted. Local debug checks are not those tests.
- Required upstream history outside retention still returns an explicit replay
  error. The code deliberately does not conceal a gap by jumping to live data.
- Source-local whole-slot ownership, Processed-only parent invalidation and strict
  recovery of old journals without owner provenance remain documented policies.
- Existing unmaintained/yanked dependency warnings remain; known vulnerabilities
  were zero in both audited locks.

## Release gates remain in place

An ordinary staged build refuses any uncommitted source changes. Do not
automatically add `--allow-dirty` or commit to bypass that refusal. A clean,
published source tree is not permission to build. Production version pins
remain unchanged. When explicitly authorized later, select a matching
unused 4.3.2 release label, build plugin and daemon together, and separately gate
switching and consumer-before-producer restarts. No release build or node operation
is part of source promotion.

## Verification

- Source-preserving independent-clone migration, portable Git provenance and
  unchanged old-checkout fingerprints.
- Local-only source-preflight and wrapper contracts, using fake inventory and
  transport stubs; no production inventory or SSH is used for these checks.
- Active staged-playbook syntax, shell syntax and the promoted source's locked
  workspace/debug/fuzz/format checks are verified before completion.

Results: **157 workspace tests passed, zero failed, one ignored timing test**;
workspace all-target and all five fuzz-bin debug checks, formatting and whitespace
checks passed. Thirty-three local entry/source contracts passed, including initial RED
cases for wrong defaults, invalid/nonportable Git metadata and missing legacy
guards, plus missing objects, nested object symlinks and a corrupt-index status
failure. Provenance outputs now use checked Git assignments rather than masking
errors inside `echo` substitutions. All three changed playbooks passed syntax
checking against a local-only
placeholder inventory; shell syntax passed. An independent rsync snapshot passed
`git fsck --full --no-reflogs`, without needing the original worktree metadata.
Both migrated locks still audit to zero known vulnerabilities; existing maintenance
and yanked warnings remain.

An independent scoped review found two additional preflight failure modes (masked
Git errors and nested metadata symlinks); both were reproduced and repaired.
The final read-only recheck reports no remaining findings, including thirteen
additional `/bin/bash` 3.2 failure-injection checks. That review does not replace
production runtime or performance acceptance.
