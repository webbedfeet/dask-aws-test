#!/bin/bash

################################################################################
# Dask Cluster Automated Setup Script (AWS Academy Learner Lab)
#
# Builds the cluster from Lecture 7 on EC2:
# - 1 scheduler node (also where you run your Dask client)
# - N worker nodes
#
# Learner Lab limits that shape this script (a full AWS account has none of
# these caps and can use bigger instances and custom IAM roles):
# - Max 9 running EC2 instances, max 32 vCPUs, sizes up to "large"
# - Region us-east-1, LabInstanceProfile for S3 access
# - Instances are STOPPED when your lab session ends and get new public IPs
#   when it restarts. Use --restart to bring the cluster back.
#
# Usage:
#   ./setup-dask-cluster.sh [N_WORKERS] [--laptop-ip IP]   # create (default 2 workers)
#   ./setup-dask-cluster.sh --restart [--laptop-ip IP]     # after a lab restart
#
# Example: ./setup-dask-cluster.sh 3
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
log_step() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# Settings (keep versions in sync with pyproject.toml)
AWS_REGION=us-east-1
CONFIG_FILE="dask-cluster-config.txt"
SCHEDULER_TYPE="t3.large"
WORKER_TYPE="m5.large"
PYTHON_VERSION="3.13"
DASK_VERSION="2026.8.0"
WORKER_THREADS=2
WORKER_MEMORY="6GB"
MAX_WORKERS=8                    # 8 workers + 1 scheduler = 9 instances (Learner Lab cap)
SSH_USER="ubuntu"
export AWS_DEFAULT_REGION=$AWS_REGION

# Parse arguments
N_WORKERS=2
LAPTOP_IP=""
RESTART=false
while [ $# -gt 0 ]; do
    case "$1" in
        --restart)    RESTART=true ;;
        --laptop-ip)  [ -n "$2" ] || { log_error "--laptop-ip needs an address"; exit 1; }
                      LAPTOP_IP="$2"; shift ;;
        -h|--help)    sed -n '3,22p' "$0"; exit 0 ;;
        ''|*[!0-9]*)  log_error "Unknown argument: $1"; exit 1 ;;
        *)            N_WORKERS=$1 ;;
    esac
    shift
done

# Wrapper so every ssh call uses the same options
ssh_cmd() {
    local host=$1; shift
    ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR -o ConnectTimeout=10 -o ServerAliveInterval=15 \
        "$SSH_USER@$host" "$@"
}

wait_for_ssh() {
    local host=$1
    for _ in $(seq 1 30); do
        if ssh_cmd "$host" true 2>/dev/null; then return 0; fi
        sleep 10
    done
    log_error "SSH to $host did not become ready"
    return 1
}

# Kill leftover dask processes. The [d] keeps the pattern from matching the
# remote shell whose command line contains this very string.
stop_dask() {
    ssh_cmd "$1" "pkill -f '[d]ask-env/bin/dask' || true"
    sleep 1
}

# Start the scheduler and workers
start_dask_processes() {
    log_info "Starting dask scheduler on $SCHEDULER_PRIVATE_IP..."
    stop_dask "$SCHEDULER_PUBLIC_IP"
    ssh_cmd "$SCHEDULER_PUBLIC_IP" "setsid nohup ~/dask-env/bin/dask scheduler --port 8786 --dashboard-address :8787 \
        > ~/dask-scheduler.log 2>&1 < /dev/null &"
    sleep 5

    local i=0
    local pub_ips=($WORKER_PUBLIC_IPS)
    for pub in "${pub_ips[@]}"; do
        i=$((i + 1))
        log_info "Starting dask worker $i on $pub..."
        stop_dask "$pub"
        ssh_cmd "$pub" "setsid nohup ~/dask-env/bin/dask worker tcp://$SCHEDULER_PRIVATE_IP:8786 \
            --nthreads $WORKER_THREADS --memory-limit $WORKER_MEMORY \
            > ~/dask-worker.log 2>&1 < /dev/null &"
    done
}

# Confirm the expected number of workers registered with the scheduler
verify_cluster() {
    log_info "Waiting for $N_WORKERS worker(s) to register..."
    local found
    found=$(ssh_cmd "$SCHEDULER_PUBLIC_IP" "~/dask-env/bin/python - <<'PY'
from dask.distributed import Client
c = Client('tcp://$SCHEDULER_PRIVATE_IP:8786', timeout=30)
try:
    c.wait_for_workers($N_WORKERS, timeout=90)
except Exception:
    pass
print(len(c.scheduler_info()['workers']))
PY" | tail -1)
    if [ "$found" = "$N_WORKERS" ]; then
        log_success "$found/$N_WORKERS workers registered"
    else
        log_error "Only ${found:-0}/$N_WORKERS workers registered. Scheduler log tail:"
        ssh_cmd "$SCHEDULER_PUBLIC_IP" "tail -20 ~/dask-scheduler.log" || true
        exit 1
    fi
}

get_ip() {   # get_ip <instance-id> <PublicIpAddress|PrivateIpAddress>
    aws ec2 describe-instances --instance-ids "$1" \
        --query "Reservations[0].Instances[0].$2" --output text
}

write_config() {
    cat > "$CONFIG_FILE" <<EOF
# Dask cluster configuration - written $(date)
AWS_REGION=$AWS_REGION
SG_ID=$SG_ID
KEY_NAME=$KEY_NAME
KEY_FILE=$KEY_FILE
SSH_USER=$SSH_USER
N_WORKERS=$N_WORKERS
DASK_VERSION=$DASK_VERSION

SCHEDULER_INSTANCE_ID=$SCHEDULER_INSTANCE_ID
SCHEDULER_PUBLIC_IP=$SCHEDULER_PUBLIC_IP
SCHEDULER_PRIVATE_IP=$SCHEDULER_PRIVATE_IP

WORKER_INSTANCE_IDS="$WORKER_INSTANCE_IDS"
WORKER_PUBLIC_IPS="$WORKER_PUBLIC_IPS"
WORKER_PRIVATE_IPS="$WORKER_PRIVATE_IPS"
EOF
}

print_next_steps() {
    log_step "Dask cluster is ready"
    cat <<EOF

1. SSH to the scheduler node (run your client HERE, not on your laptop;
   results come straight from workers, which only have private addresses):

     ssh -i $KEY_FILE $SSH_USER@$SCHEDULER_PUBLIC_IP

2. In Python on that node (use ~/dask-env/bin/python):

     from dask.distributed import Client
     client = Client("tcp://$SCHEDULER_PRIVATE_IP:8786")

3. Dashboard from your laptop (port 8787 is NOT open to the internet):

     ssh -i $KEY_FILE -L 8787:localhost:8787 $SSH_USER@$SCHEDULER_PUBLIC_IP
     then open http://localhost:8787

Reminders:
  - When your lab session ends, instances stop and IPs change.
    Next session run:  ./setup-dask-cluster.sh --restart
  - Terminate everything when done:  ./cleanup-dask-cluster.sh
  - Cluster details saved in $CONFIG_FILE
EOF
}

################################################################################
# Prerequisites
################################################################################

log_step "Checking prerequisites"

for tool in aws ssh curl; do
    command -v $tool > /dev/null || { log_error "$tool not found. Please install it first."; exit 1; }
done
aws sts get-caller-identity > /dev/null 2>&1 || {
    log_error "AWS credentials not working. Start your Learner Lab and refresh ~/.aws/credentials."
    exit 1
}
log_success "AWS CLI and credentials OK (region $AWS_REGION)"

if [ -z "$LAPTOP_IP" ]; then
    LAPTOP_IP=$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]')
fi
if ! [[ $LAPTOP_IP =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    log_error "Could not determine a valid public IP (got '$LAPTOP_IP'). Pass --laptop-ip."
    exit 1
fi
log_info "Your public IP (allowed to SSH): $LAPTOP_IP"

################################################################################
# Restart mode: reuse existing instances after a lab session restart
################################################################################

if [ "$RESTART" = true ]; then
    [ -f "$CONFIG_FILE" ] || { log_error "$CONFIG_FILE not found. Nothing to restart."; exit 1; }
    # shellcheck disable=SC1090
    source "$CONFIG_FILE"
    [ -f "$KEY_FILE" ] || { log_error "Key file $KEY_FILE not found."; exit 1; }

    log_step "Restarting existing cluster"
    ALL_IDS="$SCHEDULER_INSTANCE_ID $WORKER_INSTANCE_IDS"

    log_info "Starting instances (no-op for ones already running)..."
    # shellcheck disable=SC2086
    aws ec2 start-instances --instance-ids $ALL_IDS > /dev/null
    # shellcheck disable=SC2086
    aws ec2 wait instance-running --instance-ids $ALL_IDS

    # Allow SSH from the current laptop IP (it may have changed); ignore duplicates
    aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 22 \
        --cidr "${LAPTOP_IP}/32" > /dev/null 2>&1 || true

    SCHEDULER_PUBLIC_IP=$(get_ip "$SCHEDULER_INSTANCE_ID" PublicIpAddress)
    SCHEDULER_PRIVATE_IP=$(get_ip "$SCHEDULER_INSTANCE_ID" PrivateIpAddress)
    WORKER_PUBLIC_IPS=""; WORKER_PRIVATE_IPS=""
    for id in $WORKER_INSTANCE_IDS; do
        WORKER_PUBLIC_IPS="$WORKER_PUBLIC_IPS $(get_ip "$id" PublicIpAddress)"
        WORKER_PRIVATE_IPS="$WORKER_PRIVATE_IPS $(get_ip "$id" PrivateIpAddress)"
    done
    WORKER_PUBLIC_IPS=${WORKER_PUBLIC_IPS# }; WORKER_PRIVATE_IPS=${WORKER_PRIVATE_IPS# }

    wait_for_ssh "$SCHEDULER_PUBLIC_IP"
    for pub in $WORKER_PUBLIC_IPS; do wait_for_ssh "$pub"; done

    start_dask_processes
    write_config
    verify_cluster
    print_next_steps
    exit 0
fi

################################################################################
# Create mode
################################################################################

if [ -f "$CONFIG_FILE" ]; then
    log_error "$CONFIG_FILE already exists. Run ./cleanup-dask-cluster.sh first, or use --restart."
    exit 1
fi
if [ "$N_WORKERS" -lt 1 ] || [ "$N_WORKERS" -gt "$MAX_WORKERS" ]; then
    log_error "N_WORKERS must be between 1 and $MAX_WORKERS (Learner Lab allows 9 running instances)."
    exit 1
fi
RUNNING=$(aws ec2 describe-instances --filters "Name=instance-state-name,Values=pending,running" \
    --query 'length(Reservations[].Instances[])' --output text)
if [ $((RUNNING + N_WORKERS + 1)) -gt 9 ]; then
    log_error "$RUNNING instance(s) already running; adding $((N_WORKERS + 1)) would exceed the 9-instance limit."
    log_error "Exceeding the limit terminates instances, and 20+ instances deactivates the account."
    exit 1
fi
log_info "Plan: 1 scheduler ($SCHEDULER_TYPE) + $N_WORKERS worker(s) ($WORKER_TYPE)"

################################################################################
# Security group
################################################################################

log_step "Creating security group"

STAMP=$(date +%s)
SG_ID=$(aws ec2 create-security-group --group-name "dask-cluster-sg-$STAMP" \
    --description "Dask cluster Learner Lab" --query GroupId --output text)
log_success "Security group: $SG_ID"

# Members talk to each other on all ports (8786 scheduler, 8787 dashboard, worker ports)
aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol -1 \
    --source-group "$SG_ID" > /dev/null
# SSH only from your IP. The dashboard is reached through an SSH tunnel.
aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 22 \
    --cidr "${LAPTOP_IP}/32" > /dev/null
log_success "Rules: intra-cluster traffic, SSH from ${LAPTOP_IP}/32"

################################################################################
# Key pair
################################################################################

log_step "Creating SSH key pair"

KEY_NAME="dask-cluster-key-$STAMP"
KEY_FILE="${KEY_NAME}.pem"
aws ec2 create-key-pair --key-name "$KEY_NAME" --query KeyMaterial --output text > "$KEY_FILE"
chmod 400 "$KEY_FILE"
log_success "Saved $KEY_FILE"

################################################################################
# Launch instances
################################################################################

log_step "Launching EC2 instances"

log_info "Finding Ubuntu 22.04 AMI..."
AMI_ID=$(aws ec2 describe-images --owners 099720109477 \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
              "Name=state,Values=available" \
    --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text)
log_success "AMI: $AMI_ID"

BLOCK='[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}}]'

launch() {   # launch <type> <count> <name> <role>
    aws ec2 run-instances --image-id "$AMI_ID" --instance-type "$1" --count "$2" \
        --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
        --iam-instance-profile Name=LabInstanceProfile \
        --block-device-mappings "$BLOCK" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$3},{Key=Role,Value=$4}]" \
        --query 'Instances[].InstanceId' --output text
}

SCHEDULER_INSTANCE_ID=$(launch "$SCHEDULER_TYPE" 1 dask-scheduler dask-scheduler)
WORKER_INSTANCE_IDS=$(launch "$WORKER_TYPE" "$N_WORKERS" dask-worker dask-worker)
WORKER_INSTANCE_IDS=$(echo "$WORKER_INSTANCE_IDS" | tr '\t' ' ')
log_success "Scheduler: $SCHEDULER_INSTANCE_ID"
log_success "Workers:   $WORKER_INSTANCE_IDS"

# Save what we have so cleanup works even if a later step fails
SCHEDULER_PUBLIC_IP=""; SCHEDULER_PRIVATE_IP=""
WORKER_PUBLIC_IPS=""; WORKER_PRIVATE_IPS=""
write_config

log_info "Waiting for instances to reach running state..."
# shellcheck disable=SC2086
aws ec2 wait instance-running --instance-ids $SCHEDULER_INSTANCE_ID $WORKER_INSTANCE_IDS

SCHEDULER_PUBLIC_IP=$(get_ip "$SCHEDULER_INSTANCE_ID" PublicIpAddress)
SCHEDULER_PRIVATE_IP=$(get_ip "$SCHEDULER_INSTANCE_ID" PrivateIpAddress)
for id in $WORKER_INSTANCE_IDS; do
    WORKER_PUBLIC_IPS="$WORKER_PUBLIC_IPS $(get_ip "$id" PublicIpAddress)"
    WORKER_PRIVATE_IPS="$WORKER_PRIVATE_IPS $(get_ip "$id" PrivateIpAddress)"
done
WORKER_PUBLIC_IPS=${WORKER_PUBLIC_IPS# }; WORKER_PRIVATE_IPS=${WORKER_PRIVATE_IPS# }
write_config
log_success "Scheduler: $SCHEDULER_PUBLIC_IP (public) / $SCHEDULER_PRIVATE_IP (private)"

################################################################################
# Install Python + Dask on every node (identical versions)
################################################################################

log_step "Installing Python $PYTHON_VERSION and Dask $DASK_VERSION on all nodes"

BOOTSTRAP="set -e
command -v curl > /dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq curl; }
curl -LsSf https://astral.sh/uv/install.sh | sh > /dev/null
export PATH=\"\$HOME/.local/bin:\$PATH\"
uv venv --python $PYTHON_VERSION ~/dask-env
uv pip install --python ~/dask-env/bin/python 'dask[complete]==$DASK_VERSION' s3fs pyarrow pandas
~/dask-env/bin/python -c 'import dask, distributed; print(\"dask\", dask.__version__)'"

PIDS=()
for host in $SCHEDULER_PUBLIC_IP $WORKER_PUBLIC_IPS; do
    (
        wait_for_ssh "$host"
        ssh_cmd "$host" "$BOOTSTRAP" > "bootstrap-$host.log" 2>&1
    ) &
    PIDS+=($!)
done
FAILED=0
for pid in "${PIDS[@]}"; do wait "$pid" || FAILED=1; done
if [ $FAILED -ne 0 ]; then
    log_error "Install failed on at least one node. See bootstrap-*.log (then run ./cleanup-dask-cluster.sh)."
    exit 1
fi
rm -f bootstrap-*.log
log_success "Python and Dask installed everywhere"

################################################################################
# Start Dask and verify
################################################################################

log_step "Starting Dask scheduler and workers"
start_dask_processes
verify_cluster
print_next_steps
