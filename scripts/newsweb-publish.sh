#!/bin/bash
set -euo pipefail

cd /home/robert/projects/newsweb-runner

export GIT_SSH_COMMAND="ssh -i /home/robert/.ssh/newsweb_github -o IdentitiesOnly=yes -o BatchMode=yes"
REMOTE="git@github.com:naessrobert-ui/prisanalyse.git"
FILE="newsweb_monitor/public/latest.json"

# Unngå samtidige publiseringer
exec 9>/tmp/newsweb-publish.lock
flock -n 9 || {
    echo "En annen NewsWeb-publisering kjører allerede."
    exit 1
}

# Kontroller at ingen sporede filer er endret lokalt
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Lokale endringer finnes. Publisering stoppet."
    exit 1
fi

# Hent siste versjon fra GitHub
git fetch "$REMOTE" main

# Oppdater lokal kode når dette kan gjøres trygt
git merge --ff-only FETCH_HEAD

# Gjenopprett siste publiserte historikk
git show HEAD:"$FILE" > "$FILE"

# Slå sammen historikk og nye analyser
.venv/bin/python -m newsweb_monitor.publish_site

# Avslutt hvis ingen endringer
git diff --quiet -- "$FILE" && exit 0

# Publiser bare offentlig JSON
git add "$FILE"

git -c user.name="NewsWeb Raspberry Pi" \
    -c user.email="newsweb@users.noreply.github.com" \
    commit -m "NewsWeb: automatic update"

# Forsøk å publisere
git push "$REMOTE" HEAD:main
