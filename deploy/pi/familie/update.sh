#!/usr/bin/env bash
# Update code only. Preserve access.json, SQLite, media and Tailscale configuration.
set -euo pipefail
install_root="${MOTES_INSTALL_DIR:-$HOME/motes-familie}"
source_ref="${MOTES_REF:-main}"
source_url="https://raw.githubusercontent.com/naessrobert-ui/prisanalyse/${source_ref}"
if [[ "$(id -u)" == 0 ]]; then echo 'Kjør som vanlig bruker, ikke med sudo.' >&2; exit 1; fi
if [[ ! -f "$install_root/data/access.json" || ! -x "$install_root/venv/bin/python" || ! -d "$install_root/app/familie" ]]; then
  echo 'Fant ikke eksisterende familierom-installasjon.' >&2; exit 1
fi
umask 077
stage="$(mktemp -d "$install_root/.update.XXXXXXXX")"
backup_dir="$install_root/code-backups/$(date +%Y%m%d-%H%M%S)-$$"
stopped=false
moved=false
rollback(){
  result=$?
  trap - EXIT
  if [[ "$result" != 0 && "$stopped" == true ]]; then
    if [[ "$moved" == true ]]; then
      if [[ -d "$install_root/app/familie" ]]; then mv "$install_root/app/familie" "$stage/failed"; fi
      mv "$backup_dir" "$install_root/app/familie"
    fi
    sudo systemctl restart motes-familie || true
    echo 'Oppdateringen feilet. Forrige kode er gjenopprettet; familiekode og medier er beholdt.' >&2
  fi
  rm -rf "$stage"
  exit "$result"
}
trap rollback EXIT
mkdir "$stage/next"
for filename in __init__.py server.py notifications.py fruen.py fruen_setup.py fruen.html fruen-admin.html fruen.js index.html slekt.html sw.js requirements.txt; do
  curl --fail --silent --show-error --location "$source_url/familie/$filename" -o "$stage/next/$filename"
done
if ! cmp -s "$stage/next/requirements.txt" "$install_root/app/familie/requirements.txt"; then
  echo 'Installerer nye bildeavhengigheter før tjenesten stoppes.'
  "$install_root/venv/bin/python" -m pip install -r "$stage/next/requirements.txt"
fi
"$install_root/venv/bin/python" -m py_compile "$stage/next/server.py" "$stage/next/notifications.py" "$stage/next/fruen.py" "$stage/next/fruen_setup.py"
mkdir -p "$install_root/code-backups"
sudo systemctl stop motes-familie
stopped=true
mv "$install_root/app/familie" "$backup_dir"
moved=true
mv "$stage/next" "$install_root/app/familie"
sudo systemctl start motes-familie
for attempt in {1..20}; do
  if curl --fail --silent http://127.0.0.1:8025/familie/health; then
    echo
    echo 'Familierommet er oppdatert. Familiekode, bilder og videoklipp er beholdt.'
    stopped=false
    exit 0
  fi
  sleep 1
done
echo 'Oppstart feilet; gjenoppretter forrige kode.' >&2
exit 1
