import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

import jinja2
import yaml
from ansible.parsing.dataloader import DataLoader
from ansible.template import Templar


SOURCE = Path(__file__).resolve().parents[3]
CONTROL = Path(__file__).resolve().parents[1]
PLAYBOOK = CONTROL / 'template/2026.5.1.1510/ansible/mainnet-rpc/build_richat_staged.yml'
WRAPPER = CONTROL / 'scripts/build-richat-staged.sh'
STUBS = Path(__file__).resolve().parent / 'stubs'
play = yaml.safe_load(PLAYBOOK.read_text())[0]
task = next(item for item in play['tasks'] if item.get('name') == 'Inspect local richat source tree')
failures = []


def check(condition, name):
    print(('PASS ' if condition else 'FAIL ') + name)
    if not condition:
        failures.append(name)


def inspect(path, overrides=None):
    script = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(task['shell']).render(richat_local_src=str(path))
    return subprocess.run(['/bin/bash', '-c', script], env=dict(os.environ, **(overrides or {})), text=True, capture_output=True)


default = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(play['vars']['richat_local_src']).render(lookup=lambda kind, variable: os.environ[variable])
check(default == str(Path.home() / 'dev/claude-project/richat-shyft-4.3'), 'playbook default selects official Shyft source')
expected_commit = subprocess.check_output(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], text=True).strip()
normal = inspect(SOURCE)
check(normal.returncode == 0 and 'commit=' + expected_commit in normal.stdout, 'independent source has usable Git provenance')

with tempfile.TemporaryDirectory(prefix='richat-source-contract-') as temporary:
    directory = Path(temporary)
    fixture = directory / 'fixture'
    subprocess.run(['git', 'clone', '--no-checkout', '--no-hardlinks', str(SOURCE), str(fixture)], check=True, capture_output=True)
    linked = directory / 'linked'
    subprocess.run(['git', '-C', str(fixture), 'worktree', 'add', '--detach', str(linked), 'HEAD'], check=True, capture_output=True)
    check(inspect(linked).returncode != 0, 'linked worktree is rejected before source copy')
    plain = directory / 'not-a-repository'
    plain.mkdir()
    check(inspect(plain).returncode != 0, 'non-repository cannot pass provenance inspection')
    alternate = directory / 'alternate'
    subprocess.run(['git', 'clone', '--shared', '--no-checkout', str(SOURCE), str(alternate)], check=True, capture_output=True)
    check(inspect(alternate).returncode != 0, 'external object alternates are rejected')
    symbolic = directory / 'symbolic-git'
    symbolic.mkdir()
    (symbolic / '.git').symlink_to(SOURCE / '.git', target_is_directory=True)
    check(inspect(symbolic).returncode != 0, 'external Git directory symlink is rejected')
    empty_objects = directory / 'empty-objects'
    empty_objects.mkdir()
    check(inspect(SOURCE, {'GIT_OBJECT_DIRECTORY': str(empty_objects)}).returncode != 0, 'unavailable commit objects cannot masquerade as a clean tree')
    nested = directory / 'nested-symlink'
    subprocess.run(['git', 'clone', '--no-checkout', '--no-hardlinks', str(SOURCE), str(nested)], check=True, capture_output=True)
    shutil.rmtree(nested / '.git/objects')
    (nested / '.git/objects').symlink_to(SOURCE / '.git/objects', target_is_directory=True)
    check(inspect(nested).returncode != 0, 'nested external object-directory symlink is rejected')
    corrupt = directory / 'corrupt-index'
    subprocess.run(['git', 'clone', '--no-checkout', '--no-hardlinks', str(SOURCE), str(corrupt)], check=True, capture_output=True)
    (corrupt / '.git/index').write_bytes(b'invalid index')
    check(inspect(corrupt).returncode != 0, 'Git status failure cannot become dirty=no')
    home = directory / 'home'
    inventory = home / '.slv/inventory.mainnet.rpcs.yml'
    inventory.parent.mkdir(parents=True)
    inventory.write_text('all:\n  hosts:\n    example-rpc: {}\n')
    fake_playbook = home / '.slv/template/2026.5.1.1510/ansible/mainnet-rpc/build_richat_staged.yml'
    fake_playbook.parent.mkdir(parents=True)
    shutil.copyfile(PLAYBOOK, fake_playbook)
    arguments_file = directory / 'arguments.json'
    transport_marker = directory / 'transport-used'
    environment = dict(os.environ, HOME=str(home), PATH=str(STUBS) + os.pathsep + os.environ['PATH'], RICHAT_ARGUMENTS=str(arguments_file), RICHAT_TRANSPORT=str(transport_marker))
    help_result = subprocess.run(['/bin/bash', str(WRAPPER), '--help'], env=environment, text=True, capture_output=True)
    check(help_result.returncode == 0 and 'default ~/dev/claude-project/richat-shyft-4.3' in help_result.stdout, 'wrapper help documents official default')
    for extra, expected_dirty in [([], 'no'), (['--src', str(directory / 'source with spaces')], 'no'), (['--allow-dirty'], 'yes')]:
        result = subprocess.run(['/bin/bash', str(WRAPPER), 'example-rpc', 'richat-v11.0.0-agave4.3.2', *extra], env=environment, text=True, capture_output=True)
        check(result.returncode == 0, 'wrapper local-only contract ' + repr(extra))
        arguments = json.loads(arguments_file.read_text())
        extra_vars = {}
        for position, argument in enumerate(arguments[:-1]):
            if argument == '-e':
                extra_vars.update(json.loads(arguments[position + 1]))
        check(extra_vars.get('allow_dirty') == expected_dirty, 'dirty-tree guard preserved ' + expected_dirty)
        if '--src' in extra:
            check(extra_vars.get('richat_local_src') == extra[1], 'explicit source override remains correctly quoted')
    check(not transport_marker.exists(), 'no real or stub SSH transport invoked')

dirty_task = next(item for item in play['tasks'] if item.get('name') == 'Refuse to build an uncommitted tree (reproducibility)')
check("src_facts.dirty == 'no' or allow_dirty == 'yes'" in dirty_task['assert']['that'], 'playbook refuses dirty official source unless explicitly allowed')

for name in ['install_richat.yml', 'geyser_richat_build.yml']:
    legacy = yaml.safe_load((PLAYBOOK.parent / name).read_text())[0]
    guards = [(position, item) for position, item in enumerate(legacy['tasks']) if item.get('name') == 'Refuse the legacy tag-clone route for Agave 4.3']
    check(bool(guards), name + ' refuses the noncanonical 4.3 source route')
    if not guards:
        continue
    position, guard = guards[0]
    clone_position = next(index for index, item in enumerate(legacy['tasks']) if 'git' in item)
    check(position < clone_position, name + ' rejects before repository changes')
    condition = guard['assert']['that'][0]
    for label, expected in [('richat-v11.0.0-agave4.3', False), ('richat-v11.0.0-agave4.3.0', False), ('richat-v11.0.0-agave4.3.2-candidate', False), ('richat-v11.0.0-agave4.2', True), ('richat-v11.0.0-agave4.30', True)]:
        actual = Templar(loader=DataLoader(), variables={'richat_version_resolved': label}).template('{{ ' + condition + ' }}')
        check(actual is expected, name + ' guard policy ' + label)

print('Failed checks: ' + str(len(failures)))
raise SystemExit(1 if failures else 0)
