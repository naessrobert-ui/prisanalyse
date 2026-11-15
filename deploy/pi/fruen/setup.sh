#!/usr/bin/env bash
# Add private photo frames and visits to the existing family service; no Tailscale changes.
set -euo pipefail
if [[ "$(id -u)" == 0 ]]; then echo 'Kjør som vanlig bruker, ikke med sudo.' >&2; exit 1; fi
install_root="${MOTES_INSTALL_DIR:-$HOME/motes-familie}"
source_ref="${MOTES_REF:-main}"
if [[ ! -f "$install_root/data/access.json" ]]; then echo 'Fant ikke familierommet. Stoppet uten endringer.' >&2; exit 1; fi
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
curl --fail --silent --show-error --location "https://raw.githubusercontent.com/naessrobert-ui/prisanalyse/$source_ref/deploy/pi/familie/update.sh" -o "$stage/update.sh"
MOTES_REF="$source_ref" MOTES_INSTALL_DIR="$install_root" bash "$stage/update.sh"
FAMILY_DATA_DIR="$install_root/data" PYTHONPATH="$install_root/app" "$install_root/venv/bin/python" -m familie.fruen_setup
printf '\nÅpne https://møtes.no/fruen/admin for å invitere enheter.\n'
printf 'Velg automatisk svar bare for hennes faste bildeskjerm. Andre enheter ringer og svarer manuelt.\n'
printf 'Familiekode, bilder, videoer og eksisterende nettverksoppsett er beholdt.\n'
