#!/usr/bin/env bash
#
# realworld-test.sh — drive the RealWorldProbe end-to-end against a real
# nats-server, optionally through a real HTTP CONNECT proxy.
#
# This is the "in the real world" test for the new transport layer. It does NOT
# mock anything: a genuine nats-server WebSocket gateway is started, the probe
# connects over a real socket, and a publish/subscribe round-trip is asserted.
#
# Two modes:
#
#   ./realworld-test.sh direct
#       Start nats-server (ws://:8080) and run the probe straight at it.
#       Proves the WebSocket transport works end-to-end.
#
#   ./realworld-test.sh proxy
#       Start nats-server AND a local HTTP CONNECT proxy, then point macOS's
#       *system* Secure Web Proxy at it so ProxyResolver/NWWebSocketTransport
#       pick it up, and run the probe. The proxy's access log proves the
#       WebSocket was tunneled through CONNECT — the headline feature of this
#       branch. (macOS only; needs sudo to flip the system proxy, and the
#       script restores the previous setting on exit.)
#
# Requirements: nats-server on PATH, a Swift toolchain (run from the repo root
# so `swift run` resolves the package).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MODE="${1:-direct}"
WS_PORT=8080
PROXY_PORT=8888
NATS_URL="ws://127.0.0.1:${WS_PORT}"

command -v nats-server >/dev/null || { echo "nats-server not found on PATH"; exit 127; }

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  if [[ "${PROXY_RESTORE:-}" == "1" ]]; then
    echo "Restoring previous system proxy setting on '${NET_SERVICE}'..."
    sudo networksetup -setsecurewebproxystate "${NET_SERVICE}" off || true
  fi
}
trap cleanup EXIT

echo "Starting nats-server with WebSocket gateway on :${WS_PORT}..."
nats-server -c "${SCRIPT_DIR}/ws.conf" &
pids+=($!)
sleep 1

if [[ "$MODE" == "proxy" ]]; then
  [[ "$(uname)" == "Darwin" ]] || { echo "proxy mode is macOS-only"; exit 1; }

  # A tiny HTTP CONNECT proxy. Any real proxy works (squid, mitmproxy); we use
  # a one-file Python one so there's nothing else to install. It logs each
  # CONNECT so you can see the tunnel being established.
  echo "Starting local HTTP CONNECT proxy on :${PROXY_PORT}..."
  python3 "${SCRIPT_DIR}/connect-proxy.py" "${PROXY_PORT}" &
  pids+=($!)
  sleep 1

  NET_SERVICE="$(networksetup -listallnetworkservices | sed -n '2p')"
  echo "Pointing system Secure Web Proxy on '${NET_SERVICE}' at 127.0.0.1:${PROXY_PORT} (sudo)..."
  sudo networksetup -setsecurewebproxy "${NET_SERVICE}" 127.0.0.1 "${PROXY_PORT}"
  sudo networksetup -setsecurewebproxystate "${NET_SERVICE}" on
  PROXY_RESTORE=1
  echo "Watch the proxy log below for a CONNECT 127.0.0.1:${WS_PORT} line — that is the tunnel."
fi

echo "Running RealWorldProbe against ${NATS_URL}..."
cd "${REPO_ROOT}"
NATS_URL="${NATS_URL}" NATS_SUBJECT="probe.roundtrip" swift run RealWorldProbe
echo "Probe exited 0 — real-world round-trip succeeded."
