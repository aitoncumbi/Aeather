# Helpers for the shell-based client checks. Source this file.

mkdir -p "$WORKDIR"
cd "$WORKDIR"

# check DESCRIPTION EXPECTED ACTUAL
check() {
    if [ "$2" != "$3" ]; then
        echo "FAIL: $1: expected [$2], got [$3]" >&2
        exit 1
    fi
    echo "ok: $1"
}

# A local tree with small files, odd names and one file large enough to be
# uploaded in parts.
make_tree() {
    mkdir -p tree/sub/deeper "tree/with space"
    echo "hello from aether" > tree/hello.txt
    printf '' > tree/empty
    head -c 1048576 /dev/urandom > tree/sub/one-mib.bin
    head -c 4097 /dev/urandom > tree/sub/deeper/just-over-inline.bin
    echo "unicode" > "tree/with space/naïve café+plus.txt"
    head -c $((21 * 1048576)) /dev/urandom > tree/large.bin
}
