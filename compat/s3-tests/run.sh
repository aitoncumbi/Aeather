#!/usr/bin/env bash
# Runs Ceph's s3-tests (s3tests/functional/test_s3.py) against a fresh Aether.
#
#   compat/s3-tests/run.sh            run the tests in passing.txt; fail if any fails
#   compat/s3-tests/run.sh --all      run every test and report which pass
#   compat/s3-tests/run.sh --update   like --all, then rewrite passing.txt
#   compat/s3-tests/run.sh NAME...    run the named tests (e.g. test_bucket_list_empty)
#
# Environment: S3TESTS_DIR (a ceph/s3-tests checkout, required), AETHER (the
# binary), PYTHON (with s3-tests' requirements and pytest-timeout installed),
# JUNIT_OUT (keep the JUnit XML results there), KEEP_LOG (keep the server log),
# PASSING_OUT (with --all: write the names of passing tests there).
set -euo pipefail
cd "$(dirname "$0")/../.."
source compat/lib.sh

S3TESTS_DIR=${S3TESTS_DIR:?set S3TESTS_DIR to a ceph/s3-tests checkout}
PYTHON=${PYTHON:-python3}
PASSING=compat/s3-tests/passing.txt
MODE=${1:-passing}

start_server "${PORT:-9300}"
key() {
    "$AETHER" key create "$1" --create-buckets --import-id "$2" --import-secret "$3" > /dev/null
}
key "s3-tests main" 0555b35654ad1656d804 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
key "s3-tests alt" NOPQRSTUVWXYZABCDEFG nopqrstuvwxyzabcdefghijklmnabcdefghijklm
key "s3-tests tenant" HIJKLMNOPQRSTUVWXYZA opqrstuvwxyzabcdefghijklmnopqrstuvwxyzab
sed "s/@PORT@/${PORT:-9300}/" compat/s3-tests/s3tests.conf.template > "$WORK/s3tests.conf"

junit="$WORK/results.xml"
args=(-p no:cacheprovider -q --timeout=60 --tb=line -o junit_family=xunit1 --junitxml="$junit")
if [ "$MODE" != --all ] && [ "$MODE" != --update ]; then
    if [ "$MODE" = passing ]; then
        mapfile -t tests < <(grep -v '^#' "$PASSING" | grep . | sed 's|^|s3tests/functional/test_s3.py::|')
        echo "Running ${#tests[@]} tests from $PASSING"
    else
        tests=("${@/#/s3tests/functional/test_s3.py::}")
    fi
    set +e
    (cd "$S3TESTS_DIR" && S3TEST_CONF="$WORK/s3tests.conf" "$PYTHON" -m pytest "${args[@]}" "${tests[@]}")
    status=$?
    if [ -n "${JUNIT_OUT:-}" ]; then cp "$junit" "$JUNIT_OUT"; fi
    exit $status
fi

set +e
(cd "$S3TESTS_DIR" && S3TEST_CONF="$WORK/s3tests.conf" "$PYTHON" -m pytest "${args[@]}" s3tests/functional/test_s3.py)
set -e
if [ -n "${JUNIT_OUT:-}" ]; then cp "$junit" "$JUNIT_OUT"; fi
"$PYTHON" - "$junit" "$WORK/passing.txt" <<'PY'
import sys, xml.etree.ElementTree as ET
cases = ET.parse(sys.argv[1]).getroot().iter("testcase")
passed, failed = [], []
for case in cases:
    bad = any(child.tag in ("failure", "error", "skipped") for child in case)
    (failed if bad else passed).append(case.get("name"))
open(sys.argv[2], "w").write("".join(f"{name}\n" for name in sorted(passed)))
print(f"{len(passed)} passed, {len(failed)} failed or skipped")
PY
if [ "$MODE" = --update ]; then
    {
        echo "# s3-tests (s3tests/functional/test_s3.py) that pass against Aether."
        echo "# CI runs these and fails on any regression. Regenerate with: compat/s3-tests/run.sh --update"
        cat "$WORK/passing.txt"
    } > "$PASSING"
    echo "Updated $PASSING"
elif [ -n "${PASSING_OUT:-}" ]; then
    cp "$WORK/passing.txt" "$PASSING_OUT"
fi
