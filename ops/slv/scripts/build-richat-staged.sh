#!/usr/bin/env bash
# Build richat (geyser plugin .so + fanout daemon binary) into a staged release
# directory on <host>, from the LOCALLY MAINTAINED source tree (rsync — richat
# is our fork; no git clone). Native codegen (-C target-cpu=native).
# Does NOT touch the live plugin symlink, /home/solv/richat, geyser.json or any
# service — the node keeps running the OLD plugin/daemon until you run
# switch-richat.sh + restart.
#
# Usage:
#   ~/.slv/scripts/build-richat-staged.sh <host> [label] [--allow-dirty] [--src <path>]
#
# Examples:
#   ~/.slv/scripts/build-richat-staged.sh <host> richat-v11.0.0-agave4.3.2
#   ~/.slv/scripts/build-richat-staged.sh example-rpc          # label = mainnet_rpcs.richat_version

set -euo pipefail

usage() {
  cat <<USAGE
Usage: $0 <host> [label] [--allow-dirty] [--src <path>]

Build richat on <host> into:
  /home/solv/richat-geyser/releases/<label>/{librichat_plugin_agave.so, richat, BUILD_INFO}

What this script DOES:
  - rsync the local richat tree (default ~/dev/claude-project/richat-shyft-4.3, incl. .git)
    to /home/solv/richat-staging/<label>/src on <host>
  - cargo build --locked --release -p richat-plugin-agave AND -p richat (daemon),
    with RUSTFLAGS="-C target-cpu=native", toolchain from rust-toolchain.toml
  - copy both artifacts + BUILD_INFO and SHA256SUMS into the release dir

What this script does NOT do (intentionally):
  - swap /home/solv/librichat_plugin_agave.so / richat-geyser/current
  - replace /home/solv/richat (richat.service)
  - touch geyser.json / richat-config.yml
  - restart solv.service or richat.service
  - update versions.yml

Refuses to build when:
  - the local tree has uncommitted changes (use --allow-dirty to override)
  - Git metadata depends on a linked worktree, symlink or external object store
  - <label> is the release that is live right now (pick a new label)

The official source targets Agave 4.3.2. Pass a matching unused release label;
the default production version pins are intentionally not updated automatically.

Activate with (dry-run first):
  ~/.slv/scripts/switch-richat.sh <host> <label>
  ~/.slv/scripts/switch-richat.sh <host> <label> --apply
  ssh <host> 'sudo systemctl restart richat.service'   # daemon FIRST
  ssh <host> 'sudo systemctl restart solv.service'     # then plugin

Arguments:
  <host>          Inventory hostname (e.g. example-rpc)
  [label]         Release label, e.g. richat-v11.0.0-agave4.3.2. If omitted, uses
                  mainnet_rpcs.richat_version from ~/.slv/versions.yml.
  --allow-dirty   Build even if the local tree has uncommitted changes.
  --src <path>    Local richat tree (default ~/dev/claude-project/richat-shyft-4.3).
USAGE
}

HOST=""; VERSION_ARG=""; EXTRA_ARGS=(-e '{"allow_dirty":"no"}')
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)     usage; exit 0 ;;
    --allow-dirty) EXTRA_ARGS+=(-e '{"allow_dirty":"yes"}'); shift ;;
    --src)         EXTRA_ARGS+=(-e "$(python3 -c 'import json,os,sys; print(json.dumps({"richat_local_src": os.path.abspath(os.path.expanduser(sys.argv[1]))}))' "${2:?--src needs a path}")"); shift 2 ;;
    -*)            echo "unknown option: $1" >&2; usage; exit 1 ;;
    *)             if [[ -z "$HOST" ]]; then HOST="$1"; elif [[ -z "$VERSION_ARG" ]]; then VERSION_ARG="$1"; else echo "unexpected arg: $1" >&2; exit 1; fi; shift ;;
  esac
done

if [[ -z "$HOST" ]]; then
  usage
  exit 1
fi

if [[ ! "$HOST" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "ERROR: expected a single inventory hostname" >&2
  exit 1
fi
if [[ -n "$VERSION_ARG" && ! "$VERSION_ARG" =~ ^[A-Za-z0-9][A-Za-z0-9._+-]*$ ]]; then
  echo "ERROR: invalid Richat release label: $VERSION_ARG" >&2
  exit 1
fi

INV=~/.slv/inventory.mainnet.rpcs.yml
ANSIBLE_BASE=~/.slv/template/2026.5.1.1510/ansible
PLAYBOOK="$ANSIBLE_BASE/mainnet-rpc/build_richat_staged.yml"

if [[ ! -f "$INV" ]]; then
  echo "ERROR: inventory not found: $INV" >&2
  exit 2
fi
if [[ ! -f "$PLAYBOOK" ]]; then
  echo "ERROR: playbook not found: $PLAYBOOK" >&2
  exit 2
fi

ansible-inventory -i "$INV" --list | python3 -c '
import json,sys
inventory = json.load(sys.stdin)
hosts = {hostname for group_name, group in inventory.items() if group_name != "_meta" for hostname in group.get("hosts", [])}
sys.exit(0 if sys.argv[1] in hosts and sys.argv[1] not in inventory else "ERROR: expected an existing inventory host, not a group or pattern")
' "$HOST"

if [[ -n "$VERSION_ARG" ]]; then
  EXTRA_ARGS+=(-e "$(python3 -c 'import json,sys; print(json.dumps({"richat_version": sys.argv[1]}))' "$VERSION_ARG")")
  echo "==> Staging build of richat $VERSION_ARG (plugin + daemon) on $HOST"
else
  echo "==> Staging richat build on $HOST (label from versions.yml: mainnet_rpcs.richat_version)"
fi

echo "==> Playbook: $PLAYBOOK"
echo "==> No production state on $HOST will be modified by this run."
echo

ansible-playbook -i "$INV" "$PLAYBOOK" --limit "$HOST" "${EXTRA_ARGS[@]}"

echo
echo "==> Build done. See the build summary above. $HOST is still running the"
echo "    OLD plugin + daemon until you run switch-richat.sh and restart."
