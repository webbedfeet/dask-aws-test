# Lab: Dask on Amazon EC2 with NYC Taxi Data

In this lab you run the same analysis three ways: on your laptop, on a small Dask cluster you build on AWS EC2, and interactively in a notebook. You analyze the 2023 NYC TLC yellow taxi trip data that is already in your own S3 bucket.

## Learning objectives

- Build a Dask cluster (1 scheduler + N workers) on EC2 with a script
- Read Parquet data directly from S3 with Dask DataFrames
- Express a group-by analysis lazily and trigger it with `.compute()`
- Watch tasks run on the Dask dashboard and compare local vs. cluster runtimes
- Shut down cloud resources so you don't burn Learner Lab credits

## What you need

- An active AWS Academy Learner Lab, with credentials in `~/.aws/credentials` (refresh them each session)
- The AWS CLI, `ssh`, `scp`, `curl`, and [`uv`](https://docs.astral.sh/uv/) on your laptop
- The 2023 yellow taxi files in your S3 bucket, named `yellow_tripdata_2023-01.parquet` ... `yellow_tripdata_2023-12.parquet`, all under one prefix. Check with:

  ```bash
  aws s3 ls s3://YOUR-BUCKET/YOUR-PREFIX/
  ```

Tell the scripts where the data is. Use the bucket and prefix from the listing above, with no trailing slash:

```bash
export TAXI_S3_PATH=s3://YOUR-BUCKET/YOUR-PREFIX
```

## Files

| File | Purpose |
|------|---------|
| `setup-dask-cluster.sh` | Creates the EC2 cluster (security group, key pair, instances, Dask install) |
| `validate-dask-cluster.sh` | Checks that the cluster is healthy |
| `cleanup-dask-cluster.sh` | Terminates everything the setup script created |
| `cluster-files/pyproject.toml` | Python dependencies for your laptop |
| `cluster-files/dask_installation_test.py` | Confirms Dask works (locally or on a cluster) |
| `cluster-files/test_nyc_data_read.py` | Confirms you can read your taxi data from S3 |
| `cluster-files/nyc_tlc_problem1.py` | Problem 1 on a local Dask cluster |
| `cluster-files/nyc_tlc_problem1_cluster.py` | Problem 1 on the EC2 cluster |
| `cluster-files/nyc_tlc_problem1.ipynb` | Problem 1 as a notebook |
| `QUICK_REFERENCE.md` | One-page command cheat sheet |
| `TROUBLESHOOTING.md` | Fixes for common problems |

## The problem

Using 2023 data only, compute one row per calendar day with:

| Column | Definition |
|--------|-----------|
| `num_trips` | Number of trips |
| `mean_trip_distance` | Average `trip_distance` |
| `max_mta_tax` | Maximum `mta_tax` |
| `q95_fare_amount` | 95th percentile of `fare_amount` |
| `min_tip_amount` | Minimum `tip_amount` |
| `mean_passenger_count` | Average `passenger_count`, rounded up |

Sort by year, month, day, **descending**. The raw files contain a few trips with dates outside 2023 (the meter clock was wrong); the filter on `dt_year == 2023` removes them. Expect 365 rows.

## Part 1: Run locally

```bash
cd cluster-files
uv sync

# 1. Is Dask installed correctly?
uv run python dask_installation_test.py

# 2. Can you read your data?
uv run python test_nyc_data_read.py

# 3. Run Problem 1
uv run python nyc_tlc_problem1.py
```

This creates `daily_averages.csv`. Note the execution time printed at the end.

The script prints a dashboard link (`http://127.0.0.1:8787/status`). Open it while the job runs.

Your laptop reads from S3 using your Learner Lab credentials, so Part 1 downloads the data over your own internet connection. It can be slow on a poor connection; that is part of what you are comparing.

## Part 2: Build the cluster

From the repo root:

```bash
./setup-dask-cluster.sh 3        # 1 scheduler + 3 workers
./validate-dask-cluster.sh
```

Setup takes about 5-8 minutes. It saves everything it created in `dask-cluster-config.txt` and a `.pem` key file. Don't delete these until you have run the cleanup script.

**The Learner Lab limits you to 9 instances, so never ask for more than 8 workers.** Exceeding the limit terminates instances.

If your lab session ended and you come back later, the instances will have stopped and their IPs changed:

```bash
./setup-dask-cluster.sh --restart
```

## Part 3: Run on the cluster

Your client must run **on the scheduler node**. The workers only have private addresses, so results can't come back to your laptop directly.

```bash
source dask-cluster-config.txt

# Copy the scripts to the scheduler node
ssh -i $KEY_FILE ubuntu@$SCHEDULER_PUBLIC_IP "mkdir -p ~/dask-lab"
scp -i $KEY_FILE cluster-files/*.py ubuntu@$SCHEDULER_PUBLIC_IP:~/dask-lab/

# Log in
ssh -i $KEY_FILE ubuntu@$SCHEDULER_PUBLIC_IP
```

On the scheduler node:

```bash
cd ~/dask-lab
export TAXI_S3_PATH=s3://YOUR-BUCKET/YOUR-PREFIX
export SCHEDULER_PRIVATE_IP=<the private IP printed by setup>   # or: grep SCHEDULER_PRIVATE_IP in dask-cluster-config.txt on your laptop

~/dask-env/bin/python dask_installation_test.py tcp://$SCHEDULER_PRIVATE_IP:8786
~/dask-env/bin/python test_nyc_data_read.py
~/dask-env/bin/python nyc_tlc_problem1_cluster.py 2>&1 | tee problem1_cluster.txt
```

This creates `daily_averages_cluster.csv`.

### Watch the dashboard

In a second terminal on your laptop, open a tunnel (port 8787 is not open to the internet):

```bash
source dask-cluster-config.txt
ssh -i $KEY_FILE -L 8787:localhost:8787 ubuntu@$SCHEDULER_PUBLIC_IP
```

Then browse to <http://localhost:8787> and re-run the cluster script. Look at the **Status**, **Workers** and **Task Stream** pages. Are all workers busy? Where does the time go?

### Copy the results back

```bash
source dask-cluster-config.txt
scp -i $KEY_FILE ubuntu@$SCHEDULER_PUBLIC_IP:~/dask-lab/daily_averages_cluster.csv .
scp -i $KEY_FILE ubuntu@$SCHEDULER_PUBLIC_IP:~/dask-lab/problem1_cluster.txt .
```

## Part 4: Notebook (optional)

`cluster-files/nyc_tlc_problem1.ipynb` does the same analysis step by step. Run it on your laptop with `uv run jupyter lab`. To use the cluster, run Jupyter on the scheduler node and tunnel its port, or just run the notebook locally and compare.

## Part 5: Clean up

**Always do this when you are finished.** Running instances use your credits.

```bash
./cleanup-dask-cluster.sh
```

To pause instead (the Learner Lab stops instances when a session ends), just stop the lab and use `--restart` next time.

## Questions to answer

1. How long did Problem 1 take locally vs. on the cluster? Which was faster, and why might it not be what you expected?
2. Open the dashboard's Task Stream. How many partitions does the data have? How many tasks run in parallel?
3. Why does `nyc_tlc_year.persist()` help here? What would happen without it?
4. Our 95th-percentile calculation is exact. What is the trade-off versus Spark's `percentile_approx`?
5. What happens to the job if you remove a worker mid-run (stop one worker instance)?

## Submission

- `daily_averages.csv` and `daily_averages_cluster.csv`
- `problem1_cluster.txt` (the cluster run output)
- A screenshot of the dashboard during the cluster run
- Your answers to the questions above
- **Confirmation that you ran `cleanup-dask-cluster.sh`**
