#!/usr/bin/env python3
"""
Problem 1: Daily summaries of key metrics for NYC TLC data (CLUSTER VERSION)

This script reads 12 months of NYC TLC yellow taxi data (2023) from your S3
bucket, combines them into a single Dask DataFrame, and calculates daily
summaries on the EC2 Dask cluster. Run it ON the scheduler node.

Usage:
    export TAXI_S3_PATH=s3://your-bucket/your-prefix
    export SCHEDULER_PRIVATE_IP=xxx.xxx.xxx.xxx
    ~/dask-env/bin/python nyc_tlc_problem1_cluster.py
  or:
    ~/dask-env/bin/python nyc_tlc_problem1_cluster.py tcp://SCHEDULER_IP:8786 \\
        --s3-path s3://your-bucket/your-prefix

The files are expected at <S3_PATH>/yellow_tripdata_2023-MM.parquet
"""

import argparse
import logging
import os
import sys
import time

import numpy as np
import dask.dataframe as dd
from dask.distributed import Client

# Configure logging with basicConfig
logging.basicConfig(
    level=logging.INFO,  # Set the log level to INFO
    # Define log message format
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)

YEAR = 2023
GROUP_KEYS = ["dt_year", "dt_month", "dt_day"]


def create_dask_client(scheduler_address):
    """Connect a client to the running Dask cluster."""

    client = Client(scheduler_address, timeout=30)

    n_workers = len(client.scheduler_info()["workers"])
    logger.info(f"Connected to Dask cluster with {n_workers} workers")
    print(f"Connected to {n_workers} workers, "
          f"{sum(client.nthreads().values())} threads total")
    print("Dashboard: ssh -L 8787:localhost:8787 to the scheduler, "
          "then open http://localhost:8787")
    return client


def get_s3_paths(s3_path, months_to_read):
    """Generate S3 paths for NYC TLC data (workers read directly from S3)."""

    logger.info(f"Preparing paths for {len(months_to_read)} months of NYC TLC data")
    print(f"\nPreparing to read {len(months_to_read)} months of NYC TLC data...")
    print("=" * 60)

    base = s3_path.rstrip("/")
    paths = []

    for month_num in months_to_read:
        month_str = f"{month_num:02d}"
        path = f"{base}/yellow_tripdata_{YEAR}-{month_str}.parquet"
        paths.append(path)
        print(f"  Month {month_str}/{YEAR}: {path}")
        logger.info(f"Added path for month {month_str}: {path}")

    print(f"\n✅ Prepared {len(paths)} paths - will read directly from {base}")
    return paths


def solve_problem1(data_files, output_file):
    """
    Solve Problem 1: Calculate daily summaries of key metrics.

    Requirements:
    1. Derive dt_year, dt_month, dt_day from tpep_pickup_datetime
    2. Filter for year 2023
    3. Calculate daily summaries:
       - Number of trips
       - Average trip_distance
       - Max mta_tax
       - 95th percentile of fare_amount
       - Min tip_amount
       - Average passenger_count (rounded up)
    4. Sort by dt_year, dt_month, dt_day in descending order
    """

    logger.info("Starting Problem 1: Daily Summaries of Key Metrics")
    print("\nSolving Problem 1: Daily Summaries of Key Metrics")
    print("=" * 60)

    start_time = time.time()

    # Read all parquet files into a single DataFrame (lazy - nothing is read yet)
    columns = [
        "tpep_pickup_datetime", "trip_distance", "mta_tax",
        "fare_amount", "tip_amount", "passenger_count",
    ]
    logger.info(f"Reading {len(data_files)} parquet files into single DataFrame")
    print("Reading all data files into a single DataFrame...")
    nyc_tlc = dd.read_parquet(data_files, columns=columns)

    # Step 1: Derive date columns
    logger.info("Step 1: Deriving date columns from tpep_pickup_datetime")
    print("\nStep 1: Deriving date columns from tpep_pickup_datetime...")
    pickup = nyc_tlc["tpep_pickup_datetime"]
    nyc_tlc = nyc_tlc.assign(
        dt_year=pickup.dt.year,
        dt_month=pickup.dt.month,
        dt_day=pickup.dt.day,
    )

    # Step 2: Filter for year 2023 (the raw files contain a few stray dates)
    logger.info(f"Step 2: Filtering data for year {YEAR}")
    print(f"Step 2: Filtering data for year {YEAR}...")
    nyc_tlc_year = nyc_tlc[nyc_tlc["dt_year"] == YEAR]

    # Keep the filtered data in (distributed) memory so the two aggregations
    # below do not each re-read the files from S3
    nyc_tlc_year = nyc_tlc_year.persist()
    filtered_rows = len(nyc_tlc_year)
    logger.info(f"Filtered dataset to {filtered_rows:,} rows for year {YEAR}")
    print(f"✅ Filtered to {filtered_rows:,} rows for year {YEAR}")

    # Step 3: Calculate daily summaries
    logger.info("Step 3: Calculating daily summaries with aggregations")
    print("Step 3: Calculating daily summaries...")
    grouped = nyc_tlc_year.groupby(GROUP_KEYS)

    daily = grouped.agg(
        num_trips=("trip_distance", "size"),
        mean_trip_distance=("trip_distance", "mean"),
        max_mta_tax=("mta_tax", "max"),
        min_tip_amount=("tip_amount", "min"),
        mean_passenger_count=("passenger_count", "mean"),
    ).compute()

    # Dask has no approximate groupby quantile, so compute the exact 95th
    # percentile per day (Spark's percentile_approx is only approximate)
    q95 = (
        grouped["fare_amount"]
        .apply(lambda s: s.quantile(0.95), meta=("q95_fare_amount", "f8"))
        .compute()
        .rename("q95_fare_amount")
    )

    daily = daily.join(q95).reset_index()
    daily["mean_passenger_count"] = np.ceil(daily["mean_passenger_count"])

    # Step 4: Sort by date in descending order
    logger.info("Step 4: Sorting results by date in descending order")
    print("Step 4: Sorting by date (descending)...")
    pandas_df = (
        daily[GROUP_KEYS + [
            "num_trips", "mean_trip_distance", "max_mta_tax",
            "q95_fare_amount", "min_tip_amount", "mean_passenger_count",
        ]]
        .sort_values(GROUP_KEYS, ascending=False)
        .reset_index(drop=True)
    )

    # Display the resulting DataFrame
    logger.info("Step 5: Displaying results")
    print("\nStep 5: Displaying results...")
    print("\nTop 20 daily summaries (sorted in descending order):")
    print(pandas_df.head(20).to_string())

    total_days = len(pandas_df)
    logger.info(f"Calculated summaries for {total_days} days")
    print(f"\n✅ Calculated summaries for {total_days} days")

    # Step 6: Save to CSV (results are already a small pandas DataFrame)
    logger.info(f"Step 6: Saving results to {output_file}")
    pandas_df.to_csv(output_file, index=False)
    print(f"Step 6: ✅ Results saved to {output_file}")

    # Calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Problem 1 execution completed in {execution_time:.2f} seconds")

    # Print summary statistics
    print("\n" + "=" * 60)
    print("PROBLEM 1 COMPLETED - Summary Statistics")
    print("=" * 60)
    print(f"Total rows processed: {filtered_rows:,}")
    print(f"Days with data: {total_days}")
    print(f"Date range: {pandas_df['dt_month'].min():02d}/{pandas_df['dt_day'].min():02d}/{YEAR} "
          f"to {pandas_df['dt_month'].max():02d}/{pandas_df['dt_day'].max():02d}/{YEAR}")
    print(f"Total trips: {pandas_df['num_trips'].sum():,}")
    print(f"Average daily trips: {pandas_df['num_trips'].mean():.0f}")
    print(f"Execution time: {execution_time:.2f} seconds")

    # Show sample results
    print("\n" + "=" * 60)
    print("Sample Results (first 5 rows):")
    print("=" * 60)
    print(f"Note: Full results saved to {output_file}")

    for _, row in pandas_df.head(5).iterrows():
        print(f"Year: {int(row['dt_year'])}, Month: {int(row['dt_month']):02d}, Day: {int(row['dt_day']):02d}")
        print(f"  Trips: {int(row['num_trips']):,}")
        print(f"  Avg Distance: {row['mean_trip_distance']:.2f} miles")
        print(f"  Max MTA Tax: ${row['max_mta_tax']:.2f}")
        print(f"  95th Percentile Fare: ${row['q95_fare_amount']:.2f}")
        print(f"  Min Tip: ${row['min_tip_amount']:.2f}")
        print(f"  Avg Passengers: {row['mean_passenger_count']:.0f}")
        print("-" * 40)

    return pandas_df


def main():
    """Main function for Problem 1 - Cluster Version."""

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("scheduler", nargs="?",
                        help="Scheduler address, e.g. tcp://10.0.0.5:8786 "
                             "(default: tcp://$SCHEDULER_PRIVATE_IP:8786)")
    parser.add_argument("--s3-path", default=os.getenv("TAXI_S3_PATH"),
                        help="S3 prefix holding yellow_tripdata_2023-MM.parquet "
                             "(default: $TAXI_S3_PATH)")
    parser.add_argument("--output", default="daily_averages_cluster.csv")
    args = parser.parse_args()

    logger.info("Starting Problem 1: Daily Summaries of Key Metrics (Cluster Mode)")
    print("=" * 70)
    print("PROBLEM 1: Daily Summaries of Key Metrics (CLUSTER MODE)")
    print(f"NYC Taxi Trip Data Analysis (Jan-Dec {YEAR})")
    print("=" * 70)

    scheduler = args.scheduler
    if not scheduler and os.getenv("SCHEDULER_PRIVATE_IP"):
        scheduler = f"tcp://{os.getenv('SCHEDULER_PRIVATE_IP')}:8786"
    if not scheduler:
        print("❌ Error: Scheduler address not provided")
        print("Usage: python nyc_tlc_problem1_cluster.py tcp://SCHEDULER_IP:8786")
        print("   or: export SCHEDULER_PRIVATE_IP=xxx.xxx.xxx.xxx")
        return 1

    if not args.s3_path:
        print("❌ Error: S3 path not provided")
        print("Usage: python nyc_tlc_problem1_cluster.py --s3-path s3://your-bucket/your-prefix")
        print("   or: export TAXI_S3_PATH=s3://your-bucket/your-prefix")
        return 1

    print(f"Connecting to Dask scheduler at: {scheduler}")
    logger.info(f"Using Dask scheduler address: {scheduler}")

    overall_start = time.time()

    # Create Dask client
    logger.info("Connecting to Dask cluster")
    client = create_dask_client(scheduler)

    # Get paths for 12 months of data (full year 2023)
    months_to_read = list(range(1, 13))
    data_files = get_s3_paths(args.s3_path, months_to_read)

    # Solve Problem 1
    try:
        solve_problem1(data_files, args.output)
        success = True
        logger.info("Problem 1 analysis completed successfully")
    except Exception as e:
        logger.exception(f"Error occurred while solving Problem 1: {str(e)}")
        print(f"❌ Error solving Problem 1: {str(e)}")
        success = False

    # Clean up
    logger.info("Closing Dask client")
    client.close()

    # Overall timing
    total_time = time.time() - overall_start
    logger.info(f"Total execution time: {total_time:.2f} seconds")

    print("\n" + "=" * 70)
    if success:
        print("✅ PROBLEM 1 COMPLETED SUCCESSFULLY!")
        print(f"\nTotal execution time: {total_time:.2f} seconds")
        print("\nFiles created:")
        print(f"  - {args.output} (Problem 1 solution)")
    else:
        print("❌ Problem 1 failed - check error messages above")
    print("=" * 70)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
