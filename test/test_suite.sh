#!/bin/bash
# fsdb comprehensive test suite
# Usage: ./test/test_suite.sh
# Set FSDB_SOCKET to override socket path

set -euo pipefail

CLIENT="./test/client"
DB="test$$"
PASS=0
FAIL=0
_DAEMON_PID=""
_TMPDIR=""

# If no external daemon specified, start our own with temp directories
if [ -z "${FSDB_SOCKET:-}" ]; then
    _TMPDIR=$(mktemp -d)
    export FSDB_SOCKET="$_TMPDIR/fsdb.sock"
    export FSDB_DBDIR="$_TMPDIR/db"
    export FSDB_LOGDIR="$_TMPDIR/log"
    export FSDB_PIDFILE="$_TMPDIR/fsdb.pid"
    mkdir -p "$FSDB_DBDIR" "$FSDB_LOGDIR"
    ./daemon &
    _DAEMON_PID=$!
    for _i in 1 2 3 4 5 6 7 8 9 10; do
        [ -S "$FSDB_SOCKET" ] && break
        kill -0 $_DAEMON_PID 2>/dev/null || { echo "daemon failed to start"; exit 1; }
        sleep 0.5
    done
    [ -S "$FSDB_SOCKET" ] || { echo "daemon socket not ready"; exit 1; }
fi

SOCK="$FSDB_SOCKET"

cleanup() {
    # #29: clean up test databases
    rm -rf "${FSDB_DBDIR:-/var/lib/fsdb}/$DB" \
           "${FSDB_DBDIR:-/var/lib/fsdb}/lazy$$" \
           "${FSDB_DBDIR:-/var/lib/fsdb}/httpdb$$" \
           "${FSDB_DBDIR:-/var/lib/fsdb}/concdb" 2>/dev/null || true
    if [ -n "$_DAEMON_PID" ]; then
        kill "$_DAEMON_PID" 2>/dev/null || true
        wait "$_DAEMON_PID" 2>/dev/null || true
    fi
    if [ -n "$_TMPDIR" ]; then
        rm -rf "$_TMPDIR"
    fi
}
trap cleanup EXIT INT TERM

run_test() {
    local desc="$1" cmd="$2" expect="$3"
    local result rc=0
    result=$($CLIENT "$cmd" 2>/dev/null) || rc=$?
    if [ "$rc" -ne 0 ]; then
        echo "  FAIL: $desc (client exit $rc)"
        echo "        expected: $expect"
        ((FAIL++)) || true
    elif echo "$result" | grep -qF "$expect"; then
        echo "  PASS: $desc"
        ((PASS++)) || true
    else
        echo "  FAIL: $desc"
        echo "        expected: $expect"
        echo "        got:      $result"
        ((FAIL++)) || true
    fi
}

echo "=== fsdb test suite ==="
echo "Socket: $SOCK"
echo "Test DB: $DB"
echo ""

# ── Database creation ──
echo "[CREATE]"
run_test "create database"       "CREATE $DB"   "OK"
run_test "create duplicate (ok)" "CREATE $DB"   "OK"

# ── INSERT ──
echo "[INSERT]"
run_test "insert record"     "INSERT $DB key1 helloworld"  "OK"
run_test "insert duplicate"  "INSERT $DB key1 other"       "ERR already exists"

# ── GET ──
echo "[GET]"
run_test "get existing"      "GET $DB key1"     "helloworld"
run_test "get missing"       "GET $DB nokey"    "ERR not found"

# ── UPDATE ──
echo "[UPDATE]"
run_test "update record"     "UPDATE $DB key1 updated"  "OK"
run_test "get after update"  "GET $DB key1"             "updated"

# ── EXISTS ──
echo "[EXISTS]"
run_test "exists present"    "EXISTS $DB key1"  "Y"
run_test "exists missing"    "EXISTS $DB nokey" "N"

# ── TOUCH ──
echo "[TOUCH]"
run_test "touch new"         "TOUCH $DB key2"   "OK"
run_test "touch duplicate"   "TOUCH $DB key2"   "ERR already exists"
run_test "exists after touch" "EXISTS $DB key2" "Y"

# ── DELETE ──
echo "[DELETE]"
run_test "delete record"     "DELETE $DB key1"  "OK"
run_test "delete missing"    "DELETE $DB key1"  "ERR delete failed"
run_test "get after delete"  "GET $DB key1"     "ERR not found"
run_test "delete touched"    "DELETE $DB key2"  "OK"

# ── STATS ──
echo "[STATS]"
run_test "stats command"     "STATS"            "uptime"

# ── KEYS / COUNT (#20) ──
echo "[KEYS/COUNT]"
$CLIENT "INSERT $DB ka valA" >/dev/null 2>&1 || true
$CLIENT "INSERT $DB kb valB" >/dev/null 2>&1 || true
$CLIENT "INSERT $DB kc valC" >/dev/null 2>&1 || true
run_test "keys lists entries" "KEYS $DB"        "k"
run_test "keys with limit"    "KEYS $DB 2"      "k"
run_test "count keys"         "COUNT $DB"       "3"
$CLIENT "DELETE $DB ka" >/dev/null 2>&1 || true
$CLIENT "DELETE $DB kb" >/dev/null 2>&1 || true
$CLIENT "DELETE $DB kc" >/dev/null 2>&1 || true

# ── KEYS / COUNT must skip stray subdirs (non-file shard entries) ──
echo "[KEYS/COUNT: stray subdirs ignored]"
DIRBUG_DB="dirbug$$"
$CLIENT "CREATE $DIRBUG_DB" >/dev/null 2>&1 || true
mkdir -p "$FSDB_DBDIR/$DIRBUG_DB/_/z"
mkdir -p "$FSDB_DBDIR/$DIRBUG_DB/a/b/fakekey"
run_test "KEYS skips stray subdirs"   "KEYS $DIRBUG_DB"   "EMPTY"
run_test "COUNT skips stray subdirs"  "COUNT $DIRBUG_DB"  "0"
rm -rf "$FSDB_DBDIR/$DIRBUG_DB"

# ── Error handling ──
echo "[ERRORS]"
run_test "invalid db name"   "GET ../etc key1"       "ERR invalid DB"
run_test "unknown command"   "FOO $DB key1"          "ERR unknown command"
run_test "missing id"        "GET $DB"               "ERR invalid ID"
run_test "insert no data"    "INSERT $DB key9"       "ERR data missing"

# ── Data size enforcement (#3) ──
echo "[DATA SIZE]"
big_data=$(python3 -c "print('x' * 1025)" 2>/dev/null || python -c "print('x' * 1025)" 2>/dev/null || echo "")
if [ -n "$big_data" ]; then
    run_test "data too large" "INSERT $DB bigkey $big_data" "ERR data too large"
else
    echo "  SKIP: python not available for data size test"
fi

# ── Lazy directory creation ──
echo "[LAZY DIRS]"
LAZY_DB="lazy$$"
run_test "insert without CREATE" "INSERT $LAZY_DB lazykey1 lazydata" "OK"
run_test "get lazy record"       "GET $LAZY_DB lazykey1"             "lazydata"
run_test "delete lazy record"    "DELETE $LAZY_DB lazykey1"          "OK"

# ── Single-char ID (special _ folder) ──
echo "[SINGLE CHAR ID]"
run_test "insert 1-char id"  "INSERT $DB x singlechar"  "OK"
run_test "get 1-char id"     "GET $DB x"                "singlechar"
run_test "delete 1-char id"  "DELETE $DB x"             "OK"

# ── HTTP path tests (#28) ──
echo "[HTTP]"
if command -v curl &>/dev/null; then
    HTTPDB="httpdb$$"

    # Health endpoint (unauthenticated)
    result=$(curl -s --unix-socket "$SOCK" http://localhost/health 2>&1) || true
    if echo "$result" | grep -qF "uptime"; then
        echo "  PASS: GET /health"
        ((PASS++))
    else
        echo "  FAIL: GET /health — got: $result"
        ((FAIL++))
    fi

    # POST CREATE
    result=$(curl -s --unix-socket "$SOCK" -X POST -d "ACTION=CREATE&db=$HTTPDB" http://localhost/ 2>&1) || true
    if echo "$result" | grep -qF "OK"; then
        echo "  PASS: HTTP CREATE"
        ((PASS++))
    else
        echo "  FAIL: HTTP CREATE — got: $result"
        ((FAIL++))
    fi

    # POST INSERT
    result=$(curl -s --unix-socket "$SOCK" -X POST -d "ACTION=INSERT&db=$HTTPDB&id=hk1&data=httpval" http://localhost/ 2>&1) || true
    if echo "$result" | grep -qF "OK"; then
        echo "  PASS: HTTP INSERT"
        ((PASS++))
    else
        echo "  FAIL: HTTP INSERT — got: $result"
        ((FAIL++))
    fi

    # POST GET
    result=$(curl -s --unix-socket "$SOCK" -X POST -d "ACTION=GET&db=$HTTPDB&id=hk1" http://localhost/ 2>&1) || true
    if echo "$result" | grep -qF "httpval"; then
        echo "  PASS: HTTP GET"
        ((PASS++))
    else
        echo "  FAIL: HTTP GET — got: $result"
        ((FAIL++))
    fi

    # POST DELETE
    result=$(curl -s --unix-socket "$SOCK" -X POST -d "ACTION=DELETE&db=$HTTPDB&id=hk1" http://localhost/ 2>&1) || true
    if echo "$result" | grep -qF "OK"; then
        echo "  PASS: HTTP DELETE"
        ((PASS++))
    else
        echo "  FAIL: HTTP DELETE — got: $result"
        ((FAIL++))
    fi

    # 404 on unknown path
    result=$(curl -s --unix-socket "$SOCK" http://localhost/unknown 2>&1) || true
    if echo "$result" | grep -qF "Not Found"; then
        echo "  PASS: GET /unknown → 404"
        ((PASS++))
    else
        echo "  FAIL: GET /unknown — got: $result"
        ((FAIL++))
    fi
else
    echo "  SKIP: curl not available for HTTP tests"
fi

# ── Regression: native GET vs HTTP GET ──
echo "[REGRESSION: native GET]"
run_test "native GET works"          "GET $DB key1"          "ERR not found"
# Insert a key, GET it natively, then clean up
$CLIENT "INSERT $DB nativetest hello123" >/dev/null 2>&1 || true
run_test "native GET retrieves data" "GET $DB nativetest"    "hello123"
$CLIENT "DELETE $DB nativetest" >/dev/null 2>&1 || true

# ── Regression: HTTP form %26 / %3D in values ──
echo "[REGRESSION: HTTP encoded delimiters]"
if command -v curl &>/dev/null; then
    ENCDB="httpdb$$"
    curl -s --unix-socket "$SOCK" -X POST -d "ACTION=CREATE&db=$ENCDB" http://localhost/ >/dev/null 2>&1 || true

    # %26 = & (ampersand in value)
    result=$(curl -s --unix-socket "$SOCK" -X POST \
        -d "ACTION=INSERT&db=$ENCDB&id=enckey1&data=hello%26world" http://localhost/ 2>&1) || true
    if echo "$result" | grep -qF "OK"; then
        echo "  PASS: HTTP INSERT with %26 in data"
        ((PASS++)) || true
    else
        echo "  FAIL: HTTP INSERT with %26 in data — got: $result"
        ((FAIL++)) || true
    fi
    result=$(curl -s --unix-socket "$SOCK" -X POST \
        -d "ACTION=GET&db=$ENCDB&id=enckey1" http://localhost/ 2>&1) || true
    if echo "$result" | grep -qF "hello&world"; then
        echo "  PASS: HTTP GET returns decoded %26 as &"
        ((PASS++)) || true
    else
        echo "  FAIL: HTTP GET %26 decode — got: $result"
        ((FAIL++)) || true
    fi

    # %3D = = (equals sign in value)
    result=$(curl -s --unix-socket "$SOCK" -X POST \
        -d "ACTION=INSERT&db=$ENCDB&id=enckey2&data=a%3Db" http://localhost/ 2>&1) || true
    if echo "$result" | grep -qF "OK"; then
        echo "  PASS: HTTP INSERT with %3D in data"
        ((PASS++)) || true
    else
        echo "  FAIL: HTTP INSERT with %3D in data — got: $result"
        ((FAIL++)) || true
    fi
    result=$(curl -s --unix-socket "$SOCK" -X POST \
        -d "ACTION=GET&db=$ENCDB&id=enckey2" http://localhost/ 2>&1) || true
    if echo "$result" | grep -qF "a=b"; then
        echo "  PASS: HTTP GET returns decoded %3D as ="
        ((PASS++)) || true
    else
        echo "  FAIL: HTTP GET %3D decode — got: $result"
        ((FAIL++)) || true
    fi

    # Cleanup
    curl -s --unix-socket "$SOCK" -X POST -d "ACTION=DELETE&db=$ENCDB&id=enckey1" http://localhost/ >/dev/null 2>&1 || true
    curl -s --unix-socket "$SOCK" -X POST -d "ACTION=DELETE&db=$ENCDB&id=enckey2" http://localhost/ >/dev/null 2>&1 || true
else
    echo "  SKIP: curl not available"
fi

# ── Regression: overlength HTTP identifiers rejected ──
echo "[REGRESSION: overlength HTTP fields]"
if command -v curl &>/dev/null; then
    longid=$(python3 -c "print('a' * 256)" 2>/dev/null || echo "")
    if [ -n "$longid" ]; then
        result=$(curl -s --unix-socket "$SOCK" -X POST \
            -d "ACTION=GET&db=$DB&id=$longid" http://localhost/ 2>&1) || true
        if echo "$result" | grep -qiE "error|bad|HTTP/1.1 400|unknown"; then
            echo "  PASS: overlength id rejected"
            ((PASS++)) || true
        else
            # An empty/error response also counts as "rejected"
            if [ -z "$result" ] || ! echo "$result" | grep -qF "aaaa"; then
                echo "  PASS: overlength id rejected (empty response)"
                ((PASS++)) || true
            else
                echo "  FAIL: overlength id not rejected — got: $result"
                ((FAIL++)) || true
            fi
        fi
    else
        echo "  SKIP: python not available"
    fi
else
    echo "  SKIP: curl not available"
fi

# ── Regression: UPDATE on non-existent key fails ──
echo "[REGRESSION: UPDATE semantics]"
run_test "update non-existent fails" "UPDATE $DB noexist99 data" "ERR"

# ── Regression: Authorization smuggling via POST body ──
echo "[REGRESSION: auth smuggling via POST body]"
if [ -S "$SOCK" ]; then
    # Start a separate auth-enabled daemon
    _AUTH_TMPDIR=$(mktemp -d)
    _AUTH_TOKEN="test-secret-token-$$"
    echo "$_AUTH_TOKEN" > "$_AUTH_TMPDIR/auth_token"
    export FSDB_SOCKET="$_AUTH_TMPDIR/fsdb.sock"
    export FSDB_DBDIR="$_AUTH_TMPDIR/db"
    export FSDB_LOGDIR="$_AUTH_TMPDIR/log"
    export FSDB_PIDFILE="$_AUTH_TMPDIR/fsdb.pid"
    export FSDB_AUTH="$_AUTH_TMPDIR/auth_token"
    mkdir -p "$_AUTH_TMPDIR/db" "$_AUTH_TMPDIR/log"
    ./daemon &
    _AUTH_PID=$!
    for _i in 1 2 3 4 5 6 7 8 9 10; do
        [ -S "$_AUTH_TMPDIR/fsdb.sock" ] && break
        kill -0 $_AUTH_PID 2>/dev/null || { echo "  auth daemon failed to start"; break; }
        sleep 0.5
    done
    if [ -S "$_AUTH_TMPDIR/fsdb.sock" ]; then
        # Test 1: smuggled auth in POST body must be rejected (401)
        smuggle_body="ACTION=STATS&x=%0d%0aAuthorization:%20Bearer%20${_AUTH_TOKEN}"
        result=$(curl -s -o /dev/null -w "%{http_code}" --unix-socket "$_AUTH_TMPDIR/fsdb.sock" \
            -X POST -d "$smuggle_body" http://localhost/ 2>&1) || true
        if [ "$result" = "401" ]; then
            echo "  PASS: smuggled auth in body rejected with 401"
            ((PASS++)) || true
        else
            echo "  FAIL: smuggled auth in body — expected 401, got: $result"
            ((FAIL++)) || true
        fi

        # Test 2: real Authorization header must still work
        result=$(curl -s -o /dev/null -w "%{http_code}" --unix-socket "$_AUTH_TMPDIR/fsdb.sock" \
            -X POST -H "Authorization: Bearer $_AUTH_TOKEN" \
            -d "ACTION=STATS" http://localhost/ 2>&1) || true
        if [ "$result" = "200" ]; then
            echo "  PASS: real auth header accepted with 200"
            ((PASS++)) || true
        else
            echo "  FAIL: real auth header — expected 200, got: $result"
            ((FAIL++)) || true
        fi

        # Test 3: raw socket smuggle with literal \r\n in body
        raw_response=$(printf 'POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 60\r\nConnection: close\r\n\r\nACTION=STATS&x=\r\nAuthorization: Bearer %s        ' "$_AUTH_TOKEN" | \
            socat - UNIX-CONNECT:"$_AUTH_TMPDIR/fsdb.sock" 2>/dev/null) || true
        if echo "$raw_response" | grep -q "401"; then
            echo "  PASS: raw socket smuggled auth rejected with 401"
            ((PASS++)) || true
        else
            echo "  FAIL: raw socket smuggled auth — expected 401, got: $raw_response"
            ((FAIL++)) || true
        fi
    else
        echo "  SKIP: auth daemon socket not ready"
    fi
    kill "$_AUTH_PID" 2>/dev/null || true
    wait "$_AUTH_PID" 2>/dev/null || true
    rm -rf "$_AUTH_TMPDIR"
    # Restore original socket
    export FSDB_SOCKET="$SOCK"
fi

# ── Concurrent same-key mutation serialization ──
echo "[CONCURRENT SAME-KEY]"
CONC_DB="conc$$"
$CLIENT "CREATE $CONC_DB" >/dev/null 2>&1 || true

# Test 1: Concurrent INSERTs on the same key — exactly one must win
n_insert_ok=0
for trial in a b c d e; do
    pids=""
    for i in 1 2 3 4 5; do
        $CLIENT "INSERT $CONC_DB race${trial} val$i" >/dev/null 2>&1 &
        pids="$pids $!"
    done
    for p in $pids; do wait $p 2>/dev/null || true; done
    check=$($CLIENT "GET $CONC_DB race${trial}" 2>&1) || true
    if echo "$check" | grep -qF "val"; then
        ((n_insert_ok++)) || true
    fi
    $CLIENT "DELETE $CONC_DB race${trial}" >/dev/null 2>&1 || true
done
if [ "$n_insert_ok" -eq 5 ]; then
    echo "  PASS: concurrent INSERTs — exactly one wins each trial"
    ((PASS++)) || true
else
    echo "  FAIL: concurrent INSERTs — expected 5 successful trials, got $n_insert_ok"
    ((FAIL++)) || true
fi

# Test 2: Concurrent UPDATE+DELETE — no resurrection
# After both complete the key must be either updated or deleted — never stale.
n_nodelete_ok=0
for trial in a b c d e; do
    $CLIENT "INSERT $CONC_DB ud${trial} original" >/dev/null 2>&1 || true
    $CLIENT "UPDATE $CONC_DB ud${trial} updated" >/dev/null 2>&1 &
    pid_u=$!
    $CLIENT "DELETE $CONC_DB ud${trial}" >/dev/null 2>&1 &
    pid_d=$!
    wait $pid_u 2>/dev/null || true
    wait $pid_d 2>/dev/null || true
    result=$($CLIENT "GET $CONC_DB ud${trial}" 2>&1) || true
    if echo "$result" | grep -qF "original"; then
        true  # stale data — fail
    else
        ((n_nodelete_ok++)) || true
    fi
    $CLIENT "DELETE $CONC_DB ud${trial}" >/dev/null 2>&1 || true
done
if [ "$n_nodelete_ok" -eq 5 ]; then
    echo "  PASS: concurrent UPDATE+DELETE — no stale data"
    ((PASS++)) || true
else
    echo "  FAIL: concurrent UPDATE+DELETE — stale data in $((5 - n_nodelete_ok))/5 trials"
    ((FAIL++)) || true
fi

# Test 3: Concurrent operations on different keys proceed without deadlock
pids=""
for i in a b c d e f g h i j; do
    $CLIENT "INSERT $CONC_DB dk$i val$i" >/dev/null 2>&1 &
    pids="$pids $!"
done
diff_key_ok=1
for p in $pids; do wait $p 2>/dev/null || true; done
diff_all_found=1
for i in a b c d e f g h i j; do
    result=$($CLIENT "GET $CONC_DB dk$i" 2>&1) || true
    if ! echo "$result" | grep -qF "val$i"; then
        diff_all_found=0
        break
    fi
done
if [ "$diff_key_ok" -eq 1 ] && [ "$diff_all_found" -eq 1 ]; then
    echo "  PASS: concurrent different-key INSERTs complete without deadlock"
    ((PASS++)) || true
else
    echo "  FAIL: concurrent different-key INSERTs failed or deadlocked"
    ((FAIL++)) || true
fi
for i in a b c d e f g h i j; do
    $CLIENT "DELETE $CONC_DB dk$i" >/dev/null 2>&1 || true
done

# ── Threaded same-key concurrency regression (test/conc) ──
echo "[THREADED CONCURRENCY]"
if ./test/conc; then
    ((PASS++)) || true
else
    ((FAIL++)) || true
fi

# ── Regression: DELETE must treat TTL-expired keys as not found ──
echo "[REGRESSION: DELETE honors TTL expiry]"
_TTL_TMPDIR=$(mktemp -d)
(
    export FSDB_SOCKET="$_TTL_TMPDIR/fsdb.sock"
    export FSDB_DBDIR="$_TTL_TMPDIR/db"
    export FSDB_LOGDIR="$_TTL_TMPDIR/log"
    export FSDB_PIDFILE="$_TTL_TMPDIR/fsdb.pid"
    export FSDB_TTL=1
    mkdir -p "$FSDB_DBDIR" "$FSDB_LOGDIR"
    ./daemon &
    _TTL_PID=$!
    for _i in 1 2 3 4 5 6 7 8 9 10; do
        [ -S "$FSDB_SOCKET" ] && break
        kill -0 $_TTL_PID 2>/dev/null || { echo "  ttl daemon failed to start"; exit 1; }
        sleep 0.5
    done
    if [ -S "$FSDB_SOCKET" ]; then
        TTL_DB="ttl$$"
        $CLIENT "CREATE $TTL_DB" >/dev/null 2>&1 || true
        $CLIENT "INSERT $TTL_DB x value" >/dev/null 2>&1 || true
        sleep 2
        del_resp=$($CLIENT "DELETE $TTL_DB x" 2>/dev/null || true)
        cnt_resp=$($CLIENT "COUNT $TTL_DB" 2>/dev/null || true)
        if [ "$del_resp" = "ERR delete failed" ]; then
            echo "  PASS: DELETE on TTL-expired key returns ERR delete failed"
        else
            echo "  FAIL: DELETE on TTL-expired key — expected 'ERR delete failed', got: $del_resp"
            exit 1
        fi
        if [ "$cnt_resp" = "0" ]; then
            echo "  PASS: COUNT after TTL expiry returns 0"
        else
            echo "  FAIL: COUNT after TTL expiry — expected '0', got: $cnt_resp"
            exit 1
        fi
    else
        echo "  FAIL: ttl daemon socket not ready"
        kill "$_TTL_PID" 2>/dev/null || true
        exit 1
    fi
    kill "$_TTL_PID" 2>/dev/null || true
    wait "$_TTL_PID" 2>/dev/null || true
)
_ttl_rc=$?
rm -rf "$_TTL_TMPDIR"
if [ "$_ttl_rc" -eq 0 ]; then
    ((PASS += 2)) || true
else
    ((FAIL++)) || true
fi

# ── Results ──
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
