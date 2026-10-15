# Quick Reference: Dask Lab

## Environment

```bash
export TAXI_S3_PATH=s3://YOUR-BUCKET/YOUR-PREFIX      # 2023 files live here
source dask-cluster-config.txt                        # KEY_FILE, SCHEDULER_PUBLIC_IP, SCHEDULER_PRIVATE_IP ...
```

## Cluster lifecycle

```bash
./setup-dask-cluster.sh 3             # create: 1 scheduler + 3 workers (max 8 workers)
./setup-dask-cluster.sh --restart     # after a Learner Lab restart (new IPs)
./validate-dask-cluster.sh            # health check
./cleanup-dask-cluster.sh             # terminate everything
```

## Local

```bash
cd cluster-files && uv sync
uv run python dask_installation_test.py
uv run python test_nyc_data_read.py
uv run python nyc_tlc_problem1.py                      # -> daily_averages.csv
uv run jupyter lab
```

## Cluster

```bash
# copy files, log in
scp -i $KEY_FILE cluster-files/*.py ubuntu@$SCHEDULER_PUBLIC_IP:~/dask-lab/
ssh -i $KEY_FILE ubuntu@$SCHEDULER_PUBLIC_IP

# on the scheduler node
export TAXI_S3_PATH=s3://YOUR-BUCKET/YOUR-PREFIX
export SCHEDULER_PRIVATE_IP=10.0.x.x
~/dask-env/bin/python nyc_tlc_problem1_cluster.py      # -> daily_averages_cluster.csv

# dashboard (from your laptop), then open http://localhost:8787
ssh -i $KEY_FILE -L 8787:localhost:8787 ubuntu@$SCHEDULER_PUBLIC_IP

# fetch results
scp -i $KEY_FILE ubuntu@$SCHEDULER_PUBLIC_IP:~/dask-lab/daily_averages_cluster.csv .
```

## On the scheduler node

```bash
tail -f ~/dask-scheduler.log          # scheduler log
tail -f ~/dask-worker.log             # (on a worker node)
pgrep -af dask                        # what's running
```

## AWS

```bash
aws s3 ls $TAXI_S3_PATH/                              # list the data
aws ec2 describe-instances --filters Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].[InstanceId,Tags[?Key==`Name`]|[0].Value]' --output text
aws sts get-caller-identity                           # are credentials alive?
```

## Dask snippets

```python
from dask.distributed import Client
client = Client("tcp://<SCHEDULER_PRIVATE_IP>:8786")
client.scheduler_info()["workers"]          # who is connected
client.dashboard_link

import dask.dataframe as dd
df = dd.read_parquet(["s3://.../yellow_tripdata_2023-01.parquet", ...], columns=[...])
df.npartitions
df = df.persist()                           # keep in cluster memory
result = df.groupby("k").agg(n=("x", "size")).compute()   # -> pandas
```

## Limits (Learner Lab)

- Max 9 running instances, 32 vCPUs, sizes up to `large`; us-east-1 only
- Instances stop when the session ends, and get new public IPs when restarted
