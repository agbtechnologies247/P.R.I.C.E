#!/bin/bash
# ==============================================================================
# PRICE Engine — Delta Exchange Keepalive & Health Monitor
# Runs every 5 minutes via cron on the VPS
# Cron entry: */5 * * * * /opt/price/scripts/delta_keepalive.sh >> /var/log/price-delta-keepalive.log 2>&1
# ==============================================================================

set -euo pipefail

LOG_FILE="/var/log/price-delta-keepalive.log"
IP_CACHE_FILE="/tmp/price_vps_ip.cache"
DELTA_API_KEY="${DELTA_API_KEY:-}"
DELTA_API_SECRET="${DELTA_API_SECRET:-}"
DELTA_BASE_URL="${DELTA_BASE_URL:-https://api.india.delta.exchange}"
PRICE_SERVER_NAME="price-server"
PRICE_WORKER_NAME="price-worker"

# Load env from .env file if DELTA_API_KEY not already set
ENV_FILE="$(dirname "$(dirname "$(realpath "$0")")")/.env"
if [ -z "$DELTA_API_KEY" ] && [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)
fi

ts() { date '+%Y-%m-%d %H:%M:%S %Z'; }

echo ""
echo "======================================================="
echo "[$(ts)] PRICE Delta Keepalive Check"
echo "======================================================="

# 1. CHECK & LOG PUBLIC IP
CURRENT_IP=$(curl -sf --max-time 5 https://api.ipify.org 2>/dev/null || echo "UNKNOWN")
PREVIOUS_IP=""
if [ -f "$IP_CACHE_FILE" ]; then
    PREVIOUS_IP=$(cat "$IP_CACHE_FILE")
fi
echo "[$(ts)] VPS Public IP: $CURRENT_IP"
if [ "$CURRENT_IP" != "UNKNOWN" ]; then
    echo "$CURRENT_IP" > "$IP_CACHE_FILE"
    if [ -n "$PREVIOUS_IP" ] && [ "$CURRENT_IP" != "$PREVIOUS_IP" ]; then
        echo ""
        echo "!!! WARNING: IP CHANGE DETECTED !!!"
        echo "    OLD IP: $PREVIOUS_IP"
        echo "    NEW IP: $CURRENT_IP"
        echo "    ACTION REQUIRED: Update Delta Exchange API key IP whitelist!"
        echo "    URL: https://www.delta.exchange/app/account/manageapikeys"
        echo ""
    fi
fi

# 2. DELTA EXCHANGE API CONNECTIVITY CHECK (signed GET /v2/profile)
if [ -n "$DELTA_API_KEY" ] && command -v openssl &>/dev/null; then
    TIMESTAMP=$(date +%s)
    SIGN_DATA="GET${TIMESTAMP}/v2/profile"
    SIGNATURE=$(echo -n "$SIGN_DATA" | openssl dgst -sha256 -hmac "$DELTA_API_SECRET" | awk '{print $2}')

    HTTP_STATUS=$(curl -sf --max-time 8 \
        -H "api-key: $DELTA_API_KEY" \
        -H "signature: $SIGNATURE" \
        -H "timestamp: $TIMESTAMP" \
        -H "Content-Type: application/json" \
        -w "%{http_code}" -o /tmp/delta_health_resp.json \
        "$DELTA_BASE_URL/v2/profile" 2>/dev/null || echo "000")

    if [ "$HTTP_STATUS" = "200" ]; then
        SUCCESS=$(python3 -c "import json; d=json.load(open('/tmp/delta_health_resp.json')); print(str(d.get('success',False)).lower())" 2>/dev/null || echo "false")
        if [ "$SUCCESS" = "true" ]; then
            USER_ID=$(python3 -c "import json; d=json.load(open('/tmp/delta_health_resp.json')); print(d.get('result',{}).get('id','?'))" 2>/dev/null || echo "?")
            echo "[$(ts)] OK Delta API Connected | User ID: $USER_ID | Key: ${DELTA_API_KEY:0:8}..."
        else
            ERROR=$(python3 -c "import json; d=json.load(open('/tmp/delta_health_resp.json')); print(d.get('error',{}).get('code','unknown'))" 2>/dev/null || echo "unknown")
            echo "[$(ts)] FAIL Delta API Auth Failed: $ERROR"
            if [ "$ERROR" = "ip_not_whitelisted_for_api_key" ]; then
                echo "[$(ts)] CRITICAL: IP $CURRENT_IP not whitelisted on Delta! Update API key settings."
            fi
        fi
    else
        echo "[$(ts)] FAIL Delta API HTTP error: $HTTP_STATUS"
    fi
else
    echo "[$(ts)] SKIP DELTA_API_KEY not set or openssl missing"
fi

# 3. ACK DELTA DEADMAN SWITCH HEARTBEAT
# Delta deadman switch auto-cancels all orders if acks stop.
# This cron runs every 5 min to keep trading alive.
if [ -n "$DELTA_API_KEY" ] && command -v openssl &>/dev/null; then
    HB_TIMESTAMP=$(date +%s)
    HB_SIGN_DATA="POST${HB_TIMESTAMP}/v2/heartbeats/ack"
    HB_SIGNATURE=$(echo -n "$HB_SIGN_DATA" | openssl dgst -sha256 -hmac "$DELTA_API_SECRET" | awk '{print $2}')

    HB_STATUS=$(curl -sf --max-time 5 -X POST \
        -H "api-key: $DELTA_API_KEY" \
        -H "signature: $HB_SIGNATURE" \
        -H "timestamp: $HB_TIMESTAMP" \
        -H "Content-Type: application/json" \
        -w "%{http_code}" -o /dev/null \
        "$DELTA_BASE_URL/v2/heartbeats/ack" 2>/dev/null || echo "000")

    if [ "$HB_STATUS" = "200" ] || [ "$HB_STATUS" = "201" ]; then
        echo "[$(ts)] OK Deadman heartbeat ACK sent"
    else
        echo "[$(ts)] WARN Heartbeat ACK status: $HB_STATUS (no heartbeat registered = OK on first run)"
    fi
fi

# 4. CHECK PM2 PROCESSES AND AUTO-RESTART IF CRASHED
if command -v pm2 &>/dev/null; then
    for PROC_NAME in "$PRICE_SERVER_NAME" "$PRICE_WORKER_NAME"; do
        PROC_STATUS=$(pm2 jlist 2>/dev/null | python3 -c "
import sys, json
try:
    procs = json.load(sys.stdin)
    for p in procs:
        if p.get('name') == '$PROC_NAME':
            print(p.get('pm2_env', {}).get('status', 'unknown'))
            sys.exit(0)
    print('not_found')
except Exception as e:
    print('error')
" 2>/dev/null || echo "error")

        echo "[$(ts)] PM2 $PROC_NAME: $PROC_STATUS"
        if [ "$PROC_STATUS" = "errored" ] || [ "$PROC_STATUS" = "stopped" ]; then
            echo "[$(ts)] Restarting $PROC_NAME..."
            pm2 restart "$PROC_NAME" 2>/dev/null || true
        fi
    done
else
    echo "[$(ts)] SKIP pm2 not found"
fi

echo "[$(ts)] Keepalive check complete"
