#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
RUN_USER="${RUN_USER:-${SUDO_USER:-$(id -un)}}"
VENV_DIR="${VENV_DIR:-$REPO_DIR/.venv-prisfall}"
STATE_DIR="${STATE_DIR:-/var/lib/prisfall-time}"
if [ "$(id -u)" != 0 ]; then
  echo 'Kjør med sudo bash deploy/pi/install-prisfall-time.sh' >&2
  exit 1
fi
sudo -u "$RUN_USER" python3 -m venv "$VENV_DIR"
sudo -u "$RUN_USER" "$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/requirements-prisfall-time.txt"
install -d -o "$RUN_USER" -m 700 "$STATE_DIR"
sed -e "s#__REPO_DIR__#$REPO_DIR#g" -e "s#__USER__#$RUN_USER#g" \
    -e "s#__PYTHON__#$VENV_DIR/bin/python#g" -e "s#__STATE_DIR__#$STATE_DIR#g" \
    "$SCRIPT_DIR/prisfall-time.service" > /etc/systemd/system/prisfall-time.service
install -m 644 "$SCRIPT_DIR/prisfall-time.timer" /etc/systemd/system/prisfall-time.timer
systemctl daemon-reload
echo 'Installert, men ikke startet. Kontroller først med:'
echo 'sudo systemctl start prisfall-time.service'
echo 'sudo journalctl -u prisfall-time.service -n 80 --no-pager'
echo 'Aktiver timeplan etter vellykket første innhenting:'
echo 'sudo systemctl enable --now prisfall-time.timer'
