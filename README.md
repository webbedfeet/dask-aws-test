# Lab: Dask on Amazon EC2 with NYC Taxi Data

In this lab you run the same analysis three ways: on a single EC2 workstation instance, on a small Dask cluster you build on AWS EC2, and interactively in a notebook. You analyze the 2023 NYC TLC yellow taxi trip data that is already in your own S3 bucket.

## Learning objectives

- Build a Dask cluster (1 scheduler + N workers) on EC2 with a script
- Read Parquet data directly from S3 with Dask DataFrames
- Express a group-by analysis lazily and trigger it with `.compute()`
- Watch tasks run on the Dask dashboard and compare local vs. cluster runtimes
- Shut down cloud resources so you don't burn Learner Lab credits

## What you need

- An active AWS Academy Learner Lab
- An EC2 **workstation** instance (Ubuntu) that you SSH into from your laptop. You run all commands in this lab on it, except opening the dashboard in your browser. Your laptop only needs `ssh` and a web browser.
- On the workstation: the AWS CLI, `git`, `curl`, `ssh`, `scp` and [`uv`](https://docs.astral.sh/uv/):

  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh     # then open a new shell
  git clone https://github.com/webbedfeet/dask-aws-test.git && cd dask-aws-test
  aws sts get-caller-identity                         # must succeed
  ```

  If `aws sts get-caller-identity` fails, put your Learner Lab credentials in `~/.aws/credentials` (refresh them each session).
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
| `cluster-files/pyproject.toml` | Python dependencies for the workstation |
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

## Part 1: Run on the workstation

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

This runs a Dask cluster inside the one workstation instance, using only its cores and memory. The script prints a dashboard link like `http://127.0.0.1:8787/status`; to see it from your laptop, tunnel to the workstation (see "Watch the dashboard" below).

## Part 2: Build the cluster

From the repo root:

```bash
./setup-dask-cluster.sh 3        # 1 scheduler + 3 workers
./validate-dask-cluster.sh
```

More workers means a faster run (3-4 is a good size for this lab). Setup takes about 5-8 minutes. It saves everything it created in `dask-cluster-config.txt` and a `.pem` key file, **on the workstation**. Don't delete these until you have run the cleanup script. The script opens SSH to the cluster only from the workstation's IP, which is what you want: you reach the cluster *through* the workstation.

(The script's `--laptop-ip` option is for the machine you run it from, which here is the workstation. You normally don't need it.)

**The Learner Lab limits you to 9 instances, so never ask for more than 8 workers.** Exceeding the limit terminates instances.

If your lab session ended and you come back later, the instances will have stopped and their IPs changed:

```bash
./setup-dask-cluster.sh --restart
```

## Part 3: Run on the cluster

Your client must run **on the scheduler node**. The workers only have private addresses, so the job can't be run from the workstation either.

```bash
source dask-cluster-config.txt

# From the workstation: copy the scripts to the scheduler node
ssh -i $KEY_FILE ubuntu@$SCHEDULER_PUBLIC_IP "mkdir -p ~/dask-lab"
scp -i $KEY_FILE cluster-files/*.py ubuntu@$SCHEDULER_PUBLIC_IP:~/dask-lab/

# Log in
ssh -i $KEY_FILE ubuntu@$SCHEDULER_PUBLIC_IP
```

On the scheduler node:

```bash
cd ~/dask-lab
export TAXI_S3_PATH=s3://YOUR-BUCKET/YOUR-PREFIX
export SCHEDULER_PRIVATE_IP=<the private IP printed by setup>   # or: grep SCHEDULER_PRIVATE_IP in dask-cluster-config.txt on the workstation

~/dask-env/bin/python dask_installation_test.py tcp://$SCHEDULER_PRIVATE_IP:8786
~/dask-env/bin/python test_nyc_data_read.py
~/dask-env/bin/python nyc_tlc_problem1_cluster.py 2>&1 | tee problem1_cluster.txt
```

This creates `daily_averages_cluster.csv`.

### Watch the dashboard

Port 8787 is not open to the internet, and the cluster only accepts SSH from the workstation, so the tunnel has two hops: laptop to workstation to scheduler. In a new terminal **on your laptop** (leave it open):

```bash
ssh -i WORKSTATION-KEY.pem -L 8787:localhost:8787 ubuntu@WORKSTATION_PUBLIC_IP \
  -t "source ~/dask-aws-test/dask-cluster-config.txt && \
      ssh -i ~/dask-aws-test/\$KEY_FILE -L 8787:localhost:8787 ubuntu@\$SCHEDULER_PUBLIC_IP"
```

Replace `WORKSTATION-KEY.pem` and `WORKSTATION_PUBLIC_IP` with what you use to SSH into the workstation, and adjust `~/dask-aws-test` if you cloned elsewhere. Then browse to <http://localhost:8787> on your laptop and re-run the cluster script. Look at the **Status**, **Workers** and **Task Stream** pages. Are all workers busy? Where does the time go?

To watch the *local* run from Part 1 instead, a single hop is enough: `ssh -i WORKSTATION-KEY.pem -L 8787:localhost:8787 ubuntu@WORKSTATION_PUBLIC_IP` (use the port the script printed if it is not 8787).

### Copy the results back

From the workstation:

```bash
source dask-cluster-config.txt
scp -i $KEY_FILE ubuntu@$SCHEDULER_PUBLIC_IP:~/dask-lab/daily_averages_cluster.csv .
scp -i $KEY_FILE ubuntu@$SCHEDULER_PUBLIC_IP:~/dask-lab/problem1_cluster.txt .
```

Then download them to your laptop for submission (`scp -i WORKSTATION-KEY.pem ubuntu@WORKSTATION_PUBLIC_IP:~/dask-aws-test/'daily_averages*.csv' .`, run on your laptop).

## Part 4: Notebook (optional)

`cluster-files/nyc_tlc_problem1.ipynb` does the same analysis step by step. Run it on the workstation with `uv run jupyter lab --no-browser` and tunnel port 8888 to your laptop (`ssh -L 8888:localhost:8888 ...`). To use the cluster, run Jupyter on the scheduler node instead and chain a second tunnel as in the dashboard step.

## Part 5: Pausing and cleaning up

There are two different ways to finish, and they are not interchangeable.

**Ending a session, coming back later: do nothing to the cluster.** When your Learner Lab session ends, AWS stops the instances. Nothing is lost. Next session, from the workstation:

```bash
./setup-dask-cluster.sh --restart
```

This starts the same instances again and relaunches Dask on them. It needs `dask-cluster-config.txt` and the `.pem` key, so don't delete them.

**Finished with the lab for good: clean up.** This is permanent.

```bash
./cleanup-dask-cluster.sh
```

It terminates the instances and deletes the security group, the key pair, the `.pem` file and `dask-cluster-config.txt`. **After this you cannot `--restart`**; you would have to build a new cluster with `./setup-dask-cluster.sh`. Do not run it at the end of a session if you plan to continue tomorrow.

Always run cleanup when you are completely done: running instances use your credits. Your workstation instance keeps costing credits too, so stop it as well. After a restart the workstation's own public IP may also change.

## Questions to answer

1. How long did Problem 1 take on the single workstation vs. on the cluster? Which was faster, and why might it not be what you expected?
2. Open the dashboard's Task Stream. How many partitions does the data have? How many tasks run in parallel?
3. An earlier version of this lab computed the 95th percentile with `groupby(...).apply(quantile)`, and workers died with "exceeded 95% memory budget". Why does that need far more memory than a mean or a max? How does counting trips at each fare (in cents) avoid the problem?
4. Our 95th-percentile calculation is exact for fares that are whole cents (it rounds to the nearest cent). What is the trade-off versus Spark's `percentile_approx`, and what would change if fares had sub-cent values?
5. What happens to the job if you remove a worker mid-run (stop one worker instance)?

## Submission

- `daily_averages.csv` and `daily_averages_cluster.csv`
- `problem1_cluster.txt` (the cluster run output)
- A screenshot of the dashboard during the cluster run
- Your answers to the questions above
- **Confirmation that you ran `cleanup-dask-cluster.sh`** once you were completely finished
