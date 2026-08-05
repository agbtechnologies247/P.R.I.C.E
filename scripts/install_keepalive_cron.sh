#!/bin/bash
# ==============================================================================
# PRICE Engine — Install Delta Keepalive Cron Job on VPS
# Run ONCE on the VPS: sudo bash scripts/install_keepalive_cron.sh
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEEPALIVE_SCRIPT="$SCRIPT_DIR/delta_keepalive.sh"
LOG_DIR="/var/log"
LOG_FILE="$LOG_DIR/price-delta-keepalive.log"

echo "======================================================="
echo " PRICE Engine — Delta Keepalive Cron Installer"
echo "======================================================="

# Ensure script is executable
chmod +x "$KEEPALIVE_SCRIPT"
echo "[OK] Keepalive script is executable: $KEEPALIVE_SCRIPT"

# Create log file with right permissions
touch "$LOG_FILE" 2>/dev/null || { echo "[WARN] Cannot create $LOG_FILE — try with sudo"; }
chmod 666 "$LOG_FILE" 2>/dev/null || true
echo "[OK] Log file ready: $LOG_FILE"

# Install cron entry (runs every 5 minutes)
CRON_JOB="*/5 * * * * $KEEPALIVE_SCRIPT >> $LOG_FILE 2>&1"
CRON_COMMENT="# PRICE Engine Delta Exchange keepalive"

# Check if already installed
if crontab -l 2>/dev/null | grep -q "delta_keepalive"; then
    echo "[SKIP] Cron job already installed:"
    crontab -l 2>/dev/null | grep "delta_keepalive"
else
    # Add to crontab
    (crontab -l 2>/dev/null; echo "$CRON_COMMENT"; echo "$CRON_JOB") | crontab -
    echo "[OK] Cron job installed: $CRON_JOB"
fi

echo ""
echo "Installed crontab:"
crontab -l 2>/dev/null | grep -A1 "delta_keepalive" || echo "(none found)"

echo ""
echo "Test run (dry run):"
bash "$KEEPALIVE_SCRIPT"

echo ""
echo "======================================================="
echo " Installation complete!"
echo " Monitor: tail -f $LOG_FILE"
echo "======================================================="
