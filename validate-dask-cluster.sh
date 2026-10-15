#!/bin/bash

################################################################################
# Dask Cluster Validation Script
#
# Checks the cluster built by setup-dask-cluster.sh: instance states, SSH,
# matching package versions, worker registration, and a small distributed job.
#
# Usage: ./validate-dask-cluster.sh
################################################################################

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

CONFIG_FILE="dask-cluster-config.txt"
export AWS_DEFAULT_REGION=us-east-1
PASSED=0
FAILED=0

check_pass() { echo -e "${GREEN}✓${NC} $1"; PASSED=$((PASSED + 1)); }
check_fail() { echo -e "${RED}✗${NC} $1"; FAILED=$((FAILED + 1)); }
check_info() { echo -e "${BLUE}ℹ${NC} $1"; }

if [ ! -f "$CONFIG_FILE" ]; then
    echo "$CONFIG_FILE not found. Run ./setup-dask-cluster.sh first."
    exit 1
fi
# shellcheck disable=SC1090
source "$CONFIG_FILE"

ssh_cmd() {
    local host=$1; shift
    ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR -o ConnectTimeout=10 "$SSH_USER@$host" "$@"
}

ALL_IDS="$SCHEDULER_INSTANCE_ID $WORKER_INSTANCE_IDS"
ALL_HOSTS="$SCHEDULER_PUBLIC_IP $WORKER_PUBLIC_IPS"

echo "== Instances =="
# shellcheck disable=SC2086
STATES=$(aws ec2 describe-instances --instance-ids $ALL_IDS \
    --query 'Reservations[].Instances[].State.Name' --output text)
if [ "$(echo "$STATES" | tr '\t' '\n' | sort -u)" = "running" ]; then
    check_pass "All $(echo "$STATES" | wc -w | tr -d ' ') instances running"
else
    check_fail "Not all instances running: $STATES (try ./setup-dask-cluster.sh --restart)"
fi

echo ""
echo "== SSH and package versions =="
VERSIONS=""
for host in $ALL_HOSTS; do
    v=$(ssh_cmd "$host" "~/dask-env/bin/python -c 'import sys, dask, pandas, pyarrow; \
print(sys.version_info[:2], dask.__version__, pandas.__version__, pyarrow.__version__)'" 2> /dev/null)
    if [ -n "$v" ]; then
        check_pass "$host reachable: $v"
        VERSIONS="$VERSIONS$v"$'\n'
    else
        check_fail "$host not reachable or Dask not installed"
    fi
done
if [ "$(echo "$VERSIONS" | sed '/^$/d' | sort -u | wc -l | tr -d ' ')" = "1" ]; then
    check_pass "Versions identical on every node"
else
    check_fail "Version mismatch between nodes (Dask will warn or fail)"
fi

echo ""
echo "== Scheduler and workers =="
if ssh_cmd "$SCHEDULER_PUBLIC_IP" "pgrep -f '[d]ask-env/bin/dask' > /dev/null" 2> /dev/null; then
    check_pass "Scheduler process running"
else
    check_fail "Scheduler process not running (see ~/dask-scheduler.log on the scheduler)"
fi

RESULT=$(ssh_cmd "$SCHEDULER_PUBLIC_IP" "~/dask-env/bin/python - <<'PY'
import dask.array as da
from dask.distributed import Client
c = Client('tcp://$SCHEDULER_PRIVATE_IP:8786', timeout=30)
print('WORKERS', len(c.scheduler_info()['workers']))
x = da.ones((20000, 20000), chunks=(2000, 2000))
print('SUM', int(x.sum().compute()))
PY" 2> /dev/null)

W=$(echo "$RESULT" | awk '/^WORKERS/{print $2}')
S=$(echo "$RESULT" | awk '/^SUM/{print $2}')
if [ "$W" = "$N_WORKERS" ]; then
    check_pass "$W/$N_WORKERS workers registered"
else
    check_fail "${W:-0}/$N_WORKERS workers registered"
fi
if [ "$S" = "400000000" ]; then
    check_pass "Distributed dask.array sum correct ($S)"
else
    check_fail "Distributed computation failed (got '${S:-nothing}', expected 400000000)"
fi

echo ""
check_info "Passed: $PASSED  Failed: $FAILED"
[ "$FAILED" -eq 0 ]
