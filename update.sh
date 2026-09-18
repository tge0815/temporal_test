#!/usr/bin/env bash
# Holt den neuesten Stand und startet die Container neu.
#
#   ./update.sh                 auf main wechseln und aktualisieren
#   ./update.sh <branch>        auf einen anderen Branch wechseln und aktualisieren
#
# Die eigene .env (mit HOST_IP und Ports) bleibt dabei erhalten - auch beim
# Wechsel von einem alten Stand, in dem .env noch im Git lag.
set -euo pipefail
cd "$(dirname "$0")"

BRANCH="${1:-main}"

echo "==> Eigene Einstellungen sichern"
if [ -f .env ]; then
    cp .env .env.sicherung
    echo "    .env -> .env.sicherung"
fi

echo "==> Neuesten Stand holen"
git fetch origin --prune

# Auf alten Staenden ist .env noch versioniert. Dann wuerde git den Wechsel
# verweigern, weil die Datei lokal geaendert ist. Die Sicherung oben stellt
# die eigenen Werte danach wieder her.
if git ls-files --error-unmatch .env >/dev/null 2>&1; then
    git checkout -- .env
fi

if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
    git checkout -q "$BRANCH"
else
    git checkout -q -b "$BRANCH" "origin/$BRANCH"
fi
git pull --ff-only origin "$BRANCH"
echo "    Stand: $(git log --oneline -1)"

echo "==> Eigene Einstellungen wiederherstellen"
if [ -f .env.sicherung ]; then
    cp .env.sicherung .env
elif [ ! -f .env ]; then
    cp .env.beispiel .env
    echo "    Keine .env vorhanden - Vorlage kopiert. HOST_IP in .env eintragen!"
fi

echo "==> Container neu bauen und starten"
docker compose up --build -d --remove-orphans
docker compose ps

HOST_IP="$(grep -E '^HOST_IP=' .env | cut -d= -f2 | tr -d '[:space:]')"
PORT="$(grep -E '^DASHBOARD_PORT=' .env | cut -d= -f2 | tr -d '[:space:]')"
echo
echo "Dashboard:  http://${HOST_IP:-localhost}:${PORT:-28080}"
echo "Designer:   http://${HOST_IP:-localhost}:${PORT:-28080}/designer"
echo "Logs:       docker compose logs -f"
