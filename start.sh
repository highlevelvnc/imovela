#!/usr/bin/env bash
# ─── Imovela — local launcher ────────────────────────────────────────────────
# Single-click way to bring everything up:
#   1. Activates the venv
#   2. Pulls latest code from git (optional via SKIP_PULL=1)
#   3. Installs any new deps from requirements.txt
#   4. Runs idempotent migrations
#   5. Launches Streamlit dashboard on http://localhost:8501
#
# Usage:
#   ./start.sh            # full launch
#   SKIP_PULL=1 ./start.sh # offline mode, no git fetch
#
# Stop with Ctrl+C in the terminal where this runs.

set -e

cd "$(dirname "$0")"

# ── Colors ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
RESET='\033[0m'

echo -e "${CYAN}◆ Imovela — local launcher${RESET}"
echo

# ── 1. venv ──────────────────────────────────────────────────────────────────
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}→ Creating venv...${RESET}"
    python3 -m venv venv
fi
source venv/bin/activate

# ── 2. Pull latest ───────────────────────────────────────────────────────────
if [ -z "$SKIP_PULL" ] && command -v git >/dev/null 2>&1; then
    if git rev-parse --git-dir >/dev/null 2>&1; then
        echo -e "${CYAN}→ Pulling latest from git...${RESET}"
        git pull --ff-only 2>/dev/null || echo -e "${YELLOW}  (pull skipped — no upstream or local changes)${RESET}"
    fi
fi

# ── 3. Sync deps ─────────────────────────────────────────────────────────────
echo -e "${CYAN}→ Checking dependencies...${RESET}"
python -m pip install -q -r requirements.txt 2>&1 | grep -vE "(already satisfied|^$)" || true

# ── 4. Migrations ────────────────────────────────────────────────────────────
echo -e "${CYAN}→ Running migrations...${RESET}"
python -c "from storage.database import init_db; init_db()" 2>&1 | tail -1

# ── 5. Launch dashboard ──────────────────────────────────────────────────────
echo
echo -e "${GREEN}✓ Ready. Opening dashboard at http://localhost:8501${RESET}"
echo -e "${YELLOW}  (Ctrl+C to stop)${RESET}"
echo

# Detect if browser opening is wanted (default: yes on macOS)
if [ "$(uname -s)" = "Darwin" ] && [ -z "$NO_BROWSER" ]; then
    (sleep 2 && open http://localhost:8501) &
fi

exec python main.py dashboard
