#!/bin/bash
# ── Start the vetting API + both Streamlit apps ──────────────────────────────
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=============================================="
echo "  CLEARLINE VETTING SYSTEM"
echo "=============================================="
echo "  API      → http://localhost:8002"
echo "  API Docs → http://localhost:8002/docs"
echo "  Hospital → http://localhost:8501"
echo "  Agent    → http://localhost:8502"
echo "=============================================="

# Start the FastAPI vetting server
python -m uvicorn apis.vetting.main:app --host 0.0.0.0 --port 8002 &
API_PID=$!
echo "▶ Vetting API started (PID $API_PID)"
sleep 3

# Start Hospital app
streamlit run streamlit_vetting/hospital_app.py --server.port 8501 --server.headless true &
H_PID=$!
echo "▶ Hospital app started (PID $H_PID)"

# Start Agent app
streamlit run streamlit_vetting/agent_app.py --server.port 8502 --server.headless true &
A_PID=$!
echo "▶ Agent app started (PID $A_PID)"

echo ""
echo "All running. Press Ctrl+C to stop all."

# Cleanup on exit
trap "kill $API_PID $H_PID $A_PID 2>/dev/null; echo 'Stopped.'" EXIT
wait
