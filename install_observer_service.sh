#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="binance-prediction-observer"
SERVICE_USER="${SUDO_USER:-$(id -un)}"
VENV_DIR="$REPO_DIR/.venv"

if [[ ! -x "$VENV_DIR/bin/python" || ! -f "$REPO_DIR/observer.py" ]]; then
  echo "Run ./install_service.sh first, then rerun this script." >&2
  exit 1
fi

sudo tee "/etc/systemd/system/$SERVICE_NAME.service" >/dev/null <<EOF
[Unit]
Description=Read-only Binance prediction market anomaly observer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$REPO_DIR
ExecStart=$VENV_DIR/bin/python $REPO_DIR/observer.py
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1
NoNewPrivileges=true
ProtectSystem=full
ReadWritePaths=$REPO_DIR

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now "$SERVICE_NAME"
sudo systemctl --no-pager --full status "$SERVICE_NAME" || true
echo "Use: sudo journalctl -u $SERVICE_NAME -f"
