#!/usr/bin/env bash
# EDR Dashboard — démarrage complet avec tunnel cloudflared
set -e

VENV="/home/theod/.venv/bin/python"
PROJECT="/home/theod/python/bdd/edr"
CLOUDFLARED="$HOME/bin/cloudflared"
DASH_LOG="/tmp/edr_dashboard.log"
TUNNEL_LOG="/tmp/edr_tunnel.log"
URL_FILE="/tmp/edr_url.txt"

# ── 1. Arrêter les instances existantes ──────────────────────────────────────
pkill -f "streamlit run dashboard/app.py" 2>/dev/null || true
pkill -f "cloudflared tunnel" 2>/dev/null || true
sleep 2

# ── 2. Démarrer le dashboard Streamlit ───────────────────────────────────────
cd "$PROJECT"
nohup "$VENV" -m streamlit run dashboard/app.py \
    --server.port 8501 \
    --server.headless true \
    --server.address 0.0.0.0 \
    > "$DASH_LOG" 2>&1 &
echo "[OK] Dashboard PID=$!"
sleep 3

# ── 3. Tunnel: utiliser le token Cloudflare si configuré, sinon quick tunnel ─
if [ -f "$HOME/.cloudflare_tunnel_token" ]; then
    TOKEN=$(cat "$HOME/.cloudflare_tunnel_token")
    nohup "$CLOUDFLARED" tunnel run --token "$TOKEN" \
        > "$TUNNEL_LOG" 2>&1 &
    echo "[OK] Tunnel Cloudflare PERMANENT (PID=$!)"
    sleep 5
    echo "URL permanente liée au token — voir dash.cloudflare.com"
    echo "permanent" > "$URL_FILE"
else
    # Quick tunnel (URL temporaire mais stable tant que processus tourne)
    nohup "$CLOUDFLARED" tunnel --url localhost:8501 --no-autoupdate \
        > "$TUNNEL_LOG" 2>&1 &
    echo "[INFO] Quick tunnel démarré (PID=$!)"
    # Extraire l'URL dès qu'elle apparaît
    for i in $(seq 1 20); do
        URL=$(grep -Eo "https://[a-z0-9-]+\.trycloudflare\.com" "$TUNNEL_LOG" 2>/dev/null | tail -1)
        if [ -n "$URL" ]; then
            echo "$URL" > "$URL_FILE"
            echo ""
            echo "╔══════════════════════════════════════════════════════════════╗"
            echo "║  Dashboard en ligne :                                        ║"
            echo "║  $URL"
            echo "╚══════════════════════════════════════════════════════════════╝"
            break
        fi
        sleep 2
    done
fi
