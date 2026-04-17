#!/usr/bin/env bash
# install_refresh_timer.sh
# Installs the Smart Trader daily scriptmaster refresh as a systemd timer.
# Run once as a user with sudo privileges.
#
# Usage:  bash install_refresh_timer.sh
#         bash install_refresh_timer.sh --uninstall

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_SRC="$SCRIPT_DIR/smart-trader-refresh.service"
TIMER_SRC="$SCRIPT_DIR/smart-trader-refresh.timer"
UNIT_DIR="/etc/systemd/system"

if [[ "${1:-}" == "--uninstall" ]]; then
    echo "Disabling and removing smart-trader-refresh timer…"
    sudo systemctl stop   smart-trader-refresh.timer  2>/dev/null || true
    sudo systemctl disable smart-trader-refresh.timer 2>/dev/null || true
    sudo rm -f "$UNIT_DIR/smart-trader-refresh.service" \
               "$UNIT_DIR/smart-trader-refresh.timer"
    sudo systemctl daemon-reload
    echo "Done."
    exit 0
fi

echo "Installing smart-trader-refresh systemd timer…"
sudo cp "$SERVICE_SRC" "$UNIT_DIR/smart-trader-refresh.service"
sudo cp "$TIMER_SRC"   "$UNIT_DIR/smart-trader-refresh.timer"
sudo systemctl daemon-reload
sudo systemctl enable --now smart-trader-refresh.timer

echo ""
echo "Timer installed and started. Status:"
sudo systemctl status smart-trader-refresh.timer --no-pager
echo ""
echo "Next run:"
systemctl list-timers smart-trader-refresh.timer --no-pager 2>/dev/null || true
