#!/bin/bash

################################################################################
# Dask Cluster Cleanup Script
#
# Terminates everything created by setup-dask-cluster.sh: instances, security
# group, key pair, and the local .pem and config files.
#
# Usage: ./cleanup-dask-cluster.sh
#
# If dask-cluster-config.txt is missing, resources are found by name/tag instead.
################################################################################

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"; }

CONFIG_FILE="dask-cluster-config.txt"
export AWS_DEFAULT_REGION=us-east-1

if [ -f "$CONFIG_FILE" ]; then
    # shellcheck disable=SC1090
    source "$CONFIG_FILE"
    INSTANCE_IDS="$SCHEDULER_INSTANCE_ID $WORKER_INSTANCE_IDS"
    SG_IDS="$SG_ID"
    KEY_NAMES="$KEY_NAME"
    log_info "Loaded $CONFIG_FILE"
else
    log_warn "$CONFIG_FILE not found. Searching by tag and name instead."
    INSTANCE_IDS=$(aws ec2 describe-instances \
        --filters "Name=tag:Role,Values=dask-scheduler,dask-worker" \
                  "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query 'Reservations[].Instances[].InstanceId' --output text)
    SG_IDS=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=dask-cluster-sg-*" \
        --query 'SecurityGroups[].GroupId' --output text)
    KEY_NAMES=$(aws ec2 describe-key-pairs --filters "Name=key-name,Values=dask-cluster-key-*" \
        --query 'KeyPairs[].KeyName' --output text)
fi

echo ""
echo "=================================================="
echo "           RESOURCES TO BE DELETED"
echo "=================================================="
echo "Instances:       ${INSTANCE_IDS:-none}"
echo "Security groups: ${SG_IDS:-none}"
echo "Key pairs:       ${KEY_NAMES:-none}"
echo "=================================================="
echo ""

if [ -z "${INSTANCE_IDS// /}" ] && [ -z "${SG_IDS// /}" ] && [ -z "${KEY_NAMES// /}" ]; then
    log_info "Nothing to delete."
    rm -f "$CONFIG_FILE"
    exit 0
fi

echo "This is PERMANENT. The instances are terminated and $CONFIG_FILE and the .pem key are"
echo "deleted, so ./setup-dask-cluster.sh --restart will no longer work afterwards."
echo "If you only want to continue in a later lab session, do NOT clean up: the lab"
echo "stops the instances for you, and --restart brings them back."
echo ""
read -r -p "Are you sure you want to delete all these resources? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    log_info "Cleanup cancelled"
    exit 0
fi

# Instances
if [ -n "${INSTANCE_IDS// /}" ]; then
    log_info "Terminating instances..."
    # shellcheck disable=SC2086
    if aws ec2 terminate-instances --instance-ids $INSTANCE_IDS > /dev/null; then
        # shellcheck disable=SC2086
        aws ec2 wait instance-terminated --instance-ids $INSTANCE_IDS
        log_success "Instances terminated"
    else
        log_warn "Could not terminate some instances (already gone?). Continuing."
    fi
fi

# Security groups (retry: network interfaces can take a moment to detach)
for sg in $SG_IDS; do
    log_info "Deleting security group $sg..."
    deleted=false
    for _ in $(seq 1 12); do
        if aws ec2 delete-security-group --group-id "$sg" 2> /dev/null; then
            deleted=true; break
        fi
        sleep 10
    done
    if [ "$deleted" = true ]; then
        log_success "Deleted $sg"
    else
        log_warn "Could not delete $sg. Remove it later in the EC2 console."
    fi
done

# Key pairs and local files
for key in $KEY_NAMES; do
    aws ec2 delete-key-pair --key-name "$key" 2> /dev/null && log_success "Deleted key pair $key" \
        || log_warn "Could not delete key pair $key"
    rm -f "${key}.pem"
done
rm -f "$CONFIG_FILE" bootstrap-*.log

log_success "Cleanup complete"
log_info "Verify: aws ec2 describe-instances --filters Name=tag:Role,Values=dask-scheduler,dask-worker --query 'Reservations[].Instances[].State.Name'"
