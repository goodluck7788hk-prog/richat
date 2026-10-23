# SLV source-selection and preflight integration

These are sanitized snapshots of the operator's existing SLV controller files,
versioned with the source they select. They are not a complete SLV installation,
and copying or committing them does not run Ansible or contact any host.

## Scope

- `scripts/build-richat-staged.sh` is the local staged-build entry point.
- `template/2026.5.1.1510/ansible/mainnet-rpc/build_richat_staged.yml` selects
  `~/dev/claude-project/richat-shyft-4.3` and validates portable Git metadata,
  clean source, exact toolchain/interface pairing, QUIC safety and both locks.
- `staged_compatibility.yml` holds the shared compatibility checks.
- `install_richat.yml` and `geyser_richat_build.yml` reject the historical
  tag-clone route for Agave 4.3 before cloning. They remain snapshots of legacy
  installers, not an alternative way to deploy this maintenance branch.

Production inventories, keys, passwords, tokens, live service configuration and
controller-specific documentation are deliberately absent. The host in examples
is a placeholder. Other SLV tasks, templates and role dependencies must come from
an existing separately managed controller installation.

## Updating the controller

Review and copy only the corresponding files into an existing `~/.slv` tree.
Do not replace inventories or production version pins. The wrapper intentionally
uses that controller tree, not this bundle, when actually invoked. It builds
both plugin and daemon on a selected host, but does not switch live links or
restart services. A clean committed source is still not authorization to build.

Keep a self-contained ordinary clone: rsync includes `.git`, so linked-worktree
pointers, metadata symlinks, object alternates and Git-context overrides are
rejected. Dirty source remains rejected unless explicitly authorized separately.
The source targets Agave 4.3.2; any later authorized build needs a matching unused
release label. Switching and consumer-before-producer restarts are separate gates.

## Local-only regression checks

Use the Python environment of an existing Ansible installation, which supplies
Ansible, PyYAML and Jinja2:

```sh
python3 ops/slv/tests/test_source_contract.py
```

The harness creates only temporary local Git clones, a synthetic inventory and
transport stubs. It does not use production inventory, run real playbooks, build
artifacts, or connect over SSH. It covers default source selection, nonportable
metadata rejection, corrupt-index failure, wrapper quoting/dirty flags and both
legacy-route guards. Wrapper tests use `/bin/bash`, including macOS Bash 3.2.

## Source-only publication

Publish branch `agave-4.3` in the existing operator fork without tags or a PR.
The inherited workflows select `master`, `agave-v*` or release tags for push
builds; `agave-4.3` does not match them. Do not push tags implicitly through
`push.followTags`. Source publication does not authorize any release build,
deployment, production service operation or activation.
