#!/usr/bin/env bash
# Separate Pi service. Reuses the tested Whisper binary, never modifies family/accounting services.
set -euo pipefail
if [[ "$(id -u)" == 0 ]]; then echo 'Kjør som robert, ikke med sudo.' >&2; exit 1; fi
install_root="${MINUTES_INSTALL_DIR:-$HOME/motes-referat}"
source_ref="${MOTES_REF:-main}"
source_url="https://raw.githubusercontent.com/naessrobert-ui/prisanalyse/$source_ref"
whisper_cli="${WHISPER_CLI:-$HOME/motes-whisper-test/build/bin/whisper-cli}"
whisper_model="${WHISPER_MODEL:-$HOME/motes-whisper-test/models/ggml-base.bin}"
for tool in python3 curl sudo systemctl tailscale nice; do command -v "$tool" >/dev/null || { echo "Mangler $tool" >&2; exit 1; }; done
if [[ ! -x "$whisper_cli" || ! -s "$whisper_model" ]]; then echo 'Fant ikke den testede Whisper-installasjonen. Oppgi WHISPER_CLI og WHISPER_MODEL.' >&2; exit 1; fi
if [[ "$whisper_model" == *'.en.'* ]]; then echo 'Bruk flerspråklig modell, ikke .en.' >&2; exit 1; fi
if [[ "$install_root $whisper_cli $whisper_model" == *$'\n'* || "$install_root" == *' '* || "$whisper_cli" == *' '* || "$whisper_model" == *' '* ]]; then echo 'Installasjonsstiene må være uten mellomrom/linjeskift.' >&2; exit 1; fi
# Verify this installed CLI supports the command before changing any Funnel handler.
funnel_help="$(tailscale funnel --help 2>&1)"
if [[ "$funnel_help" != *'--https'* || "$funnel_help" != *'--bg'* ]]; then
  echo 'Kontroller tailscale funnel --help; trenger støtte for --https og --bg.' >&2; exit 1
fi
umask 077
# Refuse to overwrite a different service already using the selected public port.
serve_config="$(tailscale serve status --json)"
SERVE_CONFIG="$serve_config" python3 - <<'CHECK'
import json,os
c=json.loads(os.environ['SERVE_CONFIG'] or '{}') or {}
for host,settings in (c.get('Web') or {}).items():
    if host.endswith(':10000'):
        handlers=settings.get('Handlers') or {}
        if len(handlers)!=1 or handlers.get('/',{}).get('Proxy')!='http://127.0.0.1:8026':
            raise SystemExit('Port 10000 brukes allerede av en annen tjeneste. Ingen Funnel-endring utført.')
if '10000' in (c.get('TCP') or {}) and not any(h.endswith(':10000') for h in (c.get('Web') or {})):
    raise SystemExit('Port 10000 brukes allerede til TCP. Ingen Funnel-endring utført.')
CHECK
mkdir -p "$install_root/app" "$install_root/data"
stage="$(mktemp -d "$install_root/.stage.XXXXXXXX")"
trap 'rm -rf "$stage"' EXIT
for name in minutes_protocol.py minutes_pi.py; do
  curl --fail --silent --show-error --location "$source_url/$name" -o "$stage/$name"
done
curl --fail --silent --show-error --location "$source_url/deploy/pi/minutes/requirements.txt" -o "$stage/requirements.txt"
curl --fail --silent --show-error https://xn--mtes-gra.no/meet/minutes/public-key -o "$stage/public.json"
tailscale status --json > "$stage/tailscale.json"
python3 - "$stage/public.json" "$stage/public.key" "$stage/tailscale.json" <<'PY'
import base64,json,sys
from pathlib import Path
from urllib.parse import urlsplit
config=json.loads(Path(sys.argv[1]).read_text())
node=json.loads(Path(sys.argv[3]).read_text())['Self']['DNSName'].rstrip('.')
if urlsplit(config['origin']).hostname!=node:
    raise SystemExit('Pi-navnet stemmer ikke med nettsidens referatoppsett. Stoppet før installasjon.')
value=config['public_key']
assert len(base64.b64decode(value,validate=True))==32
Path(sys.argv[2]).write_text(value+'\n')
PY
if [[ -s "$install_root/data/public.key" ]] && ! cmp -s "$stage/public.key" "$install_root/data/public.key"; then
  echo 'Signeringsnøkkelen har endret seg. Stoppet uten å bytte betrodd nøkkel.' >&2; exit 1
fi
python3 -m venv "$install_root/venv"
"$install_root/venv/bin/python" -m pip install -q -r "$stage/requirements.txt"
"$install_root/venv/bin/python" -m py_compile "$stage/minutes_protocol.py" "$stage/minutes_pi.py"
cp "$stage/minutes_protocol.py" "$stage/minutes_pi.py" "$install_root/app/"
cp "$stage/public.key" "$install_root/data/public.key"
service_user="$(id -un)"
service_group="$(id -gn)"
unit_file="$stage/motes-referat.service"
cat > "$unit_file" <<EOF
[Unit]
Description=Local meeting transcription for motes.no
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$service_user
Group=$service_group
WorkingDirectory=$install_root/app
Environment=MINUTES_DATA_DIR=$install_root/data
Environment=MINUTES_PUBLIC_KEY_FILE=$install_root/data/public.key
Environment=WHISPER_CLI=$whisper_cli
Environment=WHISPER_MODEL=$whisper_model
Environment=MINUTES_MONTHLY_LIMIT=40
ExecStart=$install_root/venv/bin/uvicorn minutes_pi:create_app --factory --host 127.0.0.1 --port 8026 --workers 1 --no-access-log
Restart=on-failure
RestartSec=5
TimeoutStopSec=120
Nice=10
CPUQuota=180%
MemoryMax=2G
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$install_root/data

[Install]
WantedBy=multi-user.target
EOF
sudo install -m 644 "$unit_file" /etc/systemd/system/motes-referat.service
sudo systemctl daemon-reload
sudo systemctl enable motes-referat
sudo systemctl restart motes-referat
healthy=false
for attempt in {1..20}; do
  if curl --fail --silent http://127.0.0.1:8026/health; then healthy=true; break; fi
  sleep 1
done
if [[ "$healthy" != true ]]; then echo 'Oppstart feilet. Se sudo journalctl -u motes-referat -n 30 --no-pager' >&2; exit 1; fi
# A separate port preserves the existing family Funnel on 8443.
sudo tailscale funnel --bg --https=10000 http://127.0.0.1:8026
printf '\nReferattjenesten er installert. Pi-en har ingen OpenAI-nøkkel.\n'
printf 'Maks én jobb om gangen, 90 minutter per møte, 40 møter per måned.\n'
printf 'Arbeidsdata ligger i %s/data og slettes etter 24 timer.\n' "$install_root"
