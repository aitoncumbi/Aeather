# Shared helpers for the compatibility scripts: start a throwaway Aether
# server and stop it on exit. Source this file; do not run it.

AETHER=${AETHER:-aether_backend/target/release/aether}
ROOT_KEY=aether-root
ROOT_SECRET=aether-root-secret-key

# start_server PORT: runs Aether on 127.0.0.1:PORT (admin on PORT+1) with a
# fresh data directory, and waits until it is ready.
start_server() {
    local port=$1
    WORK=$(mktemp -d)
    S3_URL="http://127.0.0.1:$port"
    export AETHER_ADMIN_URL="http://127.0.0.1:$((port + 1))"
    export AETHER_ADMIN_ACCESS_KEY=$ROOT_KEY AETHER_ADMIN_SECRET_KEY=$ROOT_SECRET
    AETHER_S3__LISTEN="127.0.0.1:$port" \
    AETHER_ADMIN__LISTEN="127.0.0.1:$((port + 1))" \
    AETHER_STORAGE__DATA_DIR="$WORK/data" \
    AETHER_S3__ACCESS_KEY=$ROOT_KEY \
    AETHER_S3__SECRET_KEY=$ROOT_SECRET \
    AETHER_LOG__LEVEL=${AETHER_LOG_LEVEL:-warn} \
    AETHER_LOG__ACCESS_LOG=false \
        "$AETHER" serve > "$WORK/server.log" 2>&1 &
    SERVER_PID=$!
    trap stop_server EXIT
    for _ in $(seq 1 50); do
        if "$AETHER" health --url "$S3_URL" 2>/dev/null; then return 0; fi
        sleep 0.2
    done
    echo "Aether did not become ready; log:" >&2
    cat "$WORK/server.log" >&2
    return 1
}

stop_server() {
    if [ -n "${SERVER_PID:-}" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=
    fi
    if [ -n "${KEEP_LOG:-}" ] && [ -n "${WORK:-}" ]; then
        cp "$WORK/server.log" "$KEEP_LOG"
    fi
    rm -rf "${WORK:-}"
}
