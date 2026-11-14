"""Exercise the Pi update/rollback without invoking real sudo or systemd."""
import os
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.parametrize('mode', ['success', 'failed-health', 'failed-download'])
def test_pi_update_preserves_data_and_restores_code_on_failure(tmp_path, mode):
    repo = Path(__file__).resolve().parents[1]
    install = tmp_path / 'motes-familie'
    package = install / 'app/familie'
    package.mkdir(parents=True)
    data = install / 'data'
    data.mkdir()
    (data / 'access.json').write_text('original-private-code-hash')
    (data / 'photo.jpg').write_bytes(b'original-family-photo')
    (package / 'server.py').write_text('# previous working application\n')
    shutil.copyfile(repo / 'familie/requirements.txt', package / 'requirements.txt')
    binary = install / 'venv/bin'
    binary.mkdir(parents=True)
    (binary / 'python').symlink_to(shutil.which('python3'))
    mocks = tmp_path / 'bin'
    mocks.mkdir()
    scripts = {
        'sudo': '#!/bin/sh\necho "$*" >> "$MOCK_LOG"\nexit 0\n',
        'sleep': '#!/bin/sh\nexit 0\n',
        'curl': '''#!/usr/bin/env python3
import os,sys,shutil
args=sys.argv[1:]
if any('127.0.0.1:8025' in a for a in args):
 if os.environ['MOCK_MODE']=='failed-health':sys.exit(22)
 print('{"ok":true}');sys.exit(0)
if os.environ['MOCK_MODE']=='failed-download':sys.exit(22)
url=next(a for a in args if a.startswith('https://'))
shutil.copyfile(os.environ['MOCK_REPO']+'/familie/'+url.rsplit('/',1)[1],args[args.index('-o')+1])
''',
    }
    for name, content in scripts.items():
        (mocks / name).write_text(content)
        (mocks / name).chmod(0o755)
    log = tmp_path / 'commands.log'
    env = {**os.environ, 'PATH': str(mocks) + os.pathsep + os.environ['PATH'],
           'MOTES_INSTALL_DIR': str(install), 'MOCK_MODE': mode, 'MOCK_LOG': str(log), 'MOCK_REPO': str(repo)}
    result = subprocess.run(['bash', str(repo / 'deploy/pi/familie/update.sh')], env=env, capture_output=True, text=True, timeout=15)
    assert (data / 'access.json').read_text() == 'original-private-code-hash'
    assert (data / 'photo.jpg').read_bytes() == b'original-family-photo'
    if mode == 'success':
        assert result.returncode == 0, result.stderr
        assert (package / 'server.py').read_text() == (repo / 'familie/server.py').read_text()
        assert 'systemctl stop motes-familie' in log.read_text()
    else:
        assert result.returncode != 0
        assert (package / 'server.py').read_text() == '# previous working application\n'
        if mode == 'failed-download':
            assert not log.exists()
        else:
            assert 'systemctl restart motes-familie' in log.read_text()


@pytest.mark.parametrize('pip_ok', [True, False])
def test_dependency_install_happens_before_service_stop(tmp_path, pip_ok):
    repo = Path(__file__).resolve().parents[1]
    install = tmp_path / 'install'
    package = install / 'app/familie'
    package.mkdir(parents=True)
    (package / 'server.py').write_text('# original code\n')
    (package / 'requirements.txt').write_text('old-dependencies\n')
    (install / 'data').mkdir()
    (install / 'data/access.json').write_text('private')
    binary = install / 'venv/bin'
    binary.mkdir(parents=True)
    mocks = tmp_path / 'bin'
    mocks.mkdir()
    python = binary / 'python'
    python.write_text('#!/bin/sh\nif [ "$2" = "pip" ]; then echo pip >> "$MOCK_LOG"; exit '+('0' if pip_ok else '1')+'; fi\nexec '+shutil.which('python3')+' "$@"\n')
    python.chmod(0o755)
    scripts = {
        'sudo': '#!/bin/sh\necho "$*" >> "$MOCK_LOG"\n',
        'curl': '''#!/usr/bin/env python3
import sys,os,shutil
if any('127.0.0.1' in a for a in sys.argv):print('{"ok":true}');sys.exit(0)
url=next(a for a in sys.argv if a.startswith('https://'))
shutil.copyfile(os.environ['MOCK_REPO']+'/familie/'+url.rsplit('/',1)[1],sys.argv[sys.argv.index('-o')+1])
''',
    }
    for name, content in scripts.items():
        (mocks / name).write_text(content)
        (mocks / name).chmod(0o755)
    log = tmp_path / 'log'
    env = {**os.environ, 'PATH': str(mocks) + os.pathsep + os.environ['PATH'], 'MOTES_INSTALL_DIR': str(install), 'MOCK_LOG': str(log), 'MOCK_REPO': str(repo)}
    result = subprocess.run(['bash', str(repo / 'deploy/pi/familie/update.sh')], env=env, capture_output=True, text=True, timeout=15)
    assert (install / 'data/access.json').read_text() == 'private'
    commands = log.read_text().splitlines()
    assert commands[0] == 'pip'
    if pip_ok:
        assert result.returncode == 0, result.stderr
        assert commands[1] == 'systemctl stop motes-familie'
    else:
        assert result.returncode != 0
        assert commands == ['pip']
        assert (package / 'server.py').read_text() == '# original code\n'
