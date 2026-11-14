#!/usr/bin/env bash
# Run as the normal Pi user. Installs only this service; never resets Tailscale.
set -euo pipefail
if [[ "$(id -u)" == 0 ]]; then
  echo 'Kjør som vanlig bruker, ikke med sudo. Skriptet ber om sudo for systemd.' >&2
  exit 1
fi
install_root="${MOTES_INSTALL_DIR:-$HOME/motes-familie}"
source_ref="${MOTES_REF:-main}"
source_url="https://raw.githubusercontent.com/naessrobert-ui/prisanalyse/${source_ref}"
for binary in python3 curl systemctl; do
  command -v "$binary" >/dev/null || { echo "Mangler $binary" >&2; exit 1; }
done
if [[ "$install_root" == *[[:space:]]* ]]; then
  echo 'Installasjonsstien må være uten mellomrom.' >&2
  exit 1
fi
if [[ -f "$install_root/data/access.json" ]]; then
  echo 'En familierom-installasjon finnes allerede. Avbryter for å bevare kode og filer.' >&2
  exit 1
fi
umask 077
mkdir -p "$install_root/app/familie" "$install_root/data"
for filename in __init__.py server.py index.html requirements.txt; do
  curl --fail --silent --show-error --location "$source_url/familie/$filename" -o "$install_root/app/familie/$filename"
done
python3 -m venv "$install_root/venv" || {
  echo 'Installer først python3-venv med: sudo apt install python3-venv' >&2
  exit 1
}
"$install_root/venv/bin/pip" install -r "$install_root/app/familie/requirements.txt"
# Password is entered on /dev/tty, hashed locally and never sent to Render/GitHub/chat.
"$install_root/venv/bin/python" - "$install_root/data" <<'PY'
import getpass,json,secrets,sys,os
from pathlib import Path
import hashlib
root=Path(sys.argv[1])
while True:
    code=getpass.getpass('Velg familiekode (minst 12 tegn): ')
    if not 12<=len(code)<=200:
        print('Bruk mellom 12 og 200 tegn, gjerne flere ord.');continue
    if code!=getpass.getpass('Gjenta familiekoden: '):
        print('Kodene var ulike. Prøv igjen.');continue
    break
salt=secrets.token_hex(16)
config={'salt':salt,'code_hash':hashlib.pbkdf2_hmac('sha256',code.encode(),bytes.fromhex(salt),600000).hex(),'session_key':secrets.token_hex(32)}
path=root/'access.json'
with path.open('x') as f: json.dump(config,f)
os.chmod(path,0o600)
PY
service_user="$(id -un)"
service_group="$(id -gn)"
# A dedicated service and directory; accounting services and databases are untouched.
sudo tee /etc/systemd/system/motes-familie.service >/dev/null <<EOF
[Unit]
Description=Motes.no privat familierom
After=network.target

[Service]
Type=simple
User=$service_user
Group=$service_group
WorkingDirectory=$install_root/app
Environment=FAMILY_DATA_DIR=$install_root/data
ExecStart=$install_root/venv/bin/uvicorn familie.server:create_app --factory --host 127.0.0.1 --port 8025 --workers 1 --no-access-log
Restart=on-failure
RestartSec=5
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$install_root/data

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now motes-familie
for attempt in {1..15}; do
  if curl --fail --silent http://127.0.0.1:8025/familie/health; then
    echo
    echo "Familierommet kjører. Filer ligger på: $install_root/data"
    echo 'Neste steg: koble tjenesten til HTTPS. Ingen nettverksdeling er aktivert av dette skriptet.'
    exit 0
  fi
  sleep 1
done
echo 'Oppstart feilet. Se: sudo journalctl -u motes-familie -n 30 --no-pager' >&2
exit 1
