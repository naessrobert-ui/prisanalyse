#!/usr/bin/env bash
# ===========================================================================
# install-kupp-vakt.sh – sett opp kupp-vakt som systemd-timer på Raspberry Pi
# ===========================================================================
# Kjører kupp-vakt hvert 10. minutt (06:00-23:59) via systemd. Oppretter et
# eget virtuelt miljø med kun de lette avhengighetene kupp-vakt trenger.
#
# Bruk:
#   cd <repo>/deploy/pi
#   sudo ./install-kupp-vakt.sh            # bruker innlogget bruker + repo-rot
#   sudo REPO_DIR=/home/pi/prisanalyse RUN_USER=pi ./install-kupp-vakt.sh
#
# Etterpå: rediger /etc/kupp-vakt/kupp-vakt.env med ekte nøkler, så:
#   sudo systemctl restart kupp-vakt.timer
# ===========================================================================
set -euo pipefail

# ---- Finn repo-rot (to nivåer opp fra dette scriptet) ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"

# ---- Hvilken bruker skal tjenesten kjøre som ----
# (den som kjørte sudo, ikke root – slik at ~/.aws o.l. tilhører rett bruker)
RUN_USER="${RUN_USER:-${SUDO_USER:-$(id -un)}}"

VENV_DIR="${VENV_DIR:-$REPO_DIR/.venv-kupp}"
ENV_DIR="/etc/kupp-vakt"
ENV_FILE="$ENV_DIR/kupp-vakt.env"
SYSTEMD_DIR="/etc/systemd/system"

echo "==> Repo:        $REPO_DIR"
echo "==> Kjørebruker: $RUN_USER"
echo "==> Venv:        $VENV_DIR"

if [ ! -f "$REPO_DIR/scripts/kupp_vakt.py" ]; then
  echo "FEIL: fant ikke $REPO_DIR/scripts/kupp_vakt.py – sett REPO_DIR riktig." >&2
  exit 1
fi

# ---- 1) Virtuelt miljø + avhengigheter ----
if [ ! -d "$VENV_DIR" ]; then
  echo "==> Oppretter venv ..."
  sudo -u "$RUN_USER" python3 -m venv "$VENV_DIR"
fi
echo "==> Installerer avhengigheter (lett sett) ..."
sudo -u "$RUN_USER" "$VENV_DIR/bin/pip" install --upgrade pip >/dev/null
sudo -u "$RUN_USER" "$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/requirements-kupp-vakt.txt"
PYTHON="$VENV_DIR/bin/python"

# ---- 2) Env-fil for hemmeligheter (utenfor git-checkouten) ----
mkdir -p "$ENV_DIR"
if [ ! -f "$ENV_FILE" ]; then
  echo "==> Legger ut env-mal til $ENV_FILE (fyll inn ekte verdier!)"
  cp "$SCRIPT_DIR/kupp-vakt.env.example" "$ENV_FILE"
else
  echo "==> $ENV_FILE finnes allerede – lar den være."
fi
chown root:root "$ENV_FILE"
chmod 600 "$ENV_FILE"

# ---- 3) systemd unit-filer (med utfylte placeholdere) ----
echo "==> Installerer systemd-enheter ..."
sed -e "s#__REPO_DIR__#$REPO_DIR#g" \
    -e "s#__PYTHON__#$PYTHON#g" \
    -e "s#__USER__#$RUN_USER#g" \
    "$SCRIPT_DIR/kupp-vakt.service" > "$SYSTEMD_DIR/kupp-vakt.service"
cp "$SCRIPT_DIR/kupp-vakt.timer" "$SYSTEMD_DIR/kupp-vakt.timer"

systemctl daemon-reload
systemctl enable --now kupp-vakt.timer

echo
echo "==> Ferdig. Timeren er aktiv."
echo
echo "Neste steg:"
echo "  1) Rediger nøkler:   sudo nano $ENV_FILE"
echo "  2) Første kjøring seeder selv (markerer dagens annonser som sett, ingen"
echo "     varsler). Du kan trigge den med en gang:"
echo "       sudo systemctl start kupp-vakt.service"
echo "  3) Test at varslingskanalen virker (sender ett demo-varsel):"
echo "       cd $REPO_DIR && sudo -u $RUN_USER env \$(grep -v '^#' $ENV_FILE | xargs) $PYTHON -m scripts.kupp_vakt --test"
echo
echo "Nyttige kommandoer:"
echo "  systemctl list-timers kupp-vakt.timer     # neste kjøring"
echo "  systemctl status kupp-vakt.service        # siste kjøring"
echo "  journalctl -u kupp-vakt.service -n 50     # logg"
