# Troubleshooting: Dask Lab

## Data / S3

**`No .parquet files found` / `FileNotFoundError`**
Check the path exactly as the script builds it: `$TAXI_S3_PATH/yellow_tripdata_2023-01.parquet`.
Run `aws s3 ls $TAXI_S3_PATH/`. Common mistakes: trailing filename in `TAXI_S3_PATH`, a missing
prefix, or a `yyyy=2023/` folder in the path. If your files live somewhere different, point
`TAXI_S3_PATH` at the folder that *directly contains* the twelve files.

**`PermissionError` / `AccessDenied` / `NoCredentialsError`**
- Laptop: Learner Lab credentials expire when the lab session ends. Start the lab, copy the new
  credentials into `~/.aws/credentials`, and check with `aws sts get-caller-identity`.
- Cluster: the nodes need `LabInstanceProfile`. The setup script attaches it; if you built nodes by
  hand, attach it in the EC2 console (Actions > Security > Modify IAM role).
- Make sure your own bucket is in the same account as your Learner Lab.

**`KeyError` / `columns not found`**
Run `test_nyc_data_read.py` and compare the schema it prints to the six columns Problem 1 needs.

## Cluster

**`Config file already exists`** from `setup-dask-cluster.sh`
A previous cluster is still registered. Use `./cleanup-dask-cluster.sh` first, or `--restart` to reuse it.

**Can't connect to the scheduler** (`OSError: Timed out trying to connect`)
- Run the client on the scheduler node, not your laptop.
- Use the **private** IP: `tcp://<SCHEDULER_PRIVATE_IP>:8786`.
- Run `./validate-dask-cluster.sh`. If the scheduler is down: `./setup-dask-cluster.sh --restart`.

**Worker/scheduler version-mismatch warnings**
Your client must use the same Dask and Python as the cluster. On the scheduler node use `~/dask-env/bin/python`, not the system `python3`.

**Fewer workers than expected**
`tail ~/dask-worker.log` on that worker. Memory-limit kills show up as `Worker exceeded 95% memory`. Fix by using more workers, or fewer partitions per worker, not by editing code.

**Everything stopped / IPs changed**
The Learner Lab stops instances when a session ends. Start the lab, refresh credentials, then `./setup-dask-cluster.sh --restart`. If your own IP changed, the script re-opens SSH for it.

**SSH: `Connection timed out`**
Your laptop IP changed (new network, VPN). Run `./setup-dask-cluster.sh --restart --laptop-ip $(curl -s https://checkip.amazonaws.com)`.

**`Permission denied (publickey)`**
Wrong key or permissions: `chmod 400 *.pem` and use the key file named in `dask-cluster-config.txt`.

## Dashboard

**http://localhost:8787 doesn't load**
The tunnel terminal must stay open. If port 8787 is busy locally, tunnel to another port: `-L 9787:localhost:8787` and browse to `localhost:9787`.

## Performance

**Slow locally, but not on the cluster (or vice versa)?** That is the point of the lab. Local reads pull data over your internet connection, while cluster nodes read S3 over AWS's network.

**Job runs out of memory**
Close other applications. Locally, edit `memory_limit` in `create_dask_client()`. Only read the columns you need (the scripts already pass `columns=[...]`).

## Cost

Instances cost credits while running. `./cleanup-dask-cluster.sh` removes instances, the security group and the key pair. Check afterward:

```bash
aws ec2 describe-instances --filters Name=instance-state-name,Values=running,pending \
  --query 'length(Reservations[].Instances[])'
```
