#!/usr/bin/env python3
"""
NYC TLC Data Reading Test Script

This script tests reading the 2023 NYC TLC data from your S3 bucket and
diagnoses any issues.

Usage:
    export TAXI_S3_PATH=s3://your-bucket/your-prefix
    python test_nyc_data_read.py
  or:
    python test_nyc_data_read.py s3://your-bucket/your-prefix
"""

import os
import sys

import dask.dataframe as dd
import fsspec


def test_nyc_data_access(s3_path):
    """Test NYC TLC data access and diagnose issues."""

    print("Testing NYC TLC Data Access...")
    print("=" * 50)
    base = s3_path.rstrip("/")
    print(f"Base path: {base}")

    # Test 1: list the location
    print("\n1. Listing files at the path...")
    try:
        fs, root = fsspec.core.url_to_fs(base)
        files = sorted(fs.glob(f"{root}/*.parquet"))
    except Exception as e:
        print(f"   ❌ Could not list {base}: {e}")
        print("\n   Check: bucket name/prefix typo, AWS credentials (locally) or")
        print("   LabInstanceProfile (on EC2), and that the Learner Lab is running.")
        return False

    if not files:
        print(f"   ❌ No .parquet files found under {base}")
        print("   Check the prefix with:  aws s3 ls " + base + "/")
        return False
    print(f"   ✅ Found {len(files)} parquet files:")
    for f in files[:15]:
        print(f"      - {os.path.basename(f)}")

    expected = [f"yellow_tripdata_2023-{m:02d}.parquet" for m in range(1, 13)]
    present = {os.path.basename(f) for f in files}
    missing = [e for e in expected if e not in present]
    if missing:
        print(f"   ⚠️  Missing expected files: {', '.join(missing)}")
    else:
        print("   ✅ All 12 months of 2023 are present")

    # Test 2: read one file
    print("\n2. Reading one month (January 2023)...")
    jan = f"{base}/yellow_tripdata_2023-01.parquet"
    try:
        df = dd.read_parquet(jan)
        print(f"   ✅ SUCCESS: {len(df):,} rows, {df.npartitions} partition(s)")
        print("   Schema:")
        for col, dtype in df.dtypes.items():
            print(f"      {col}: {dtype}")
        print("   Sample data:")
        print(df.head(5).to_string())
    except Exception as e:
        print(f"   ❌ Read failed: {e}")
        return False

    # Test 3: columns Problem 1 needs
    print("\n3. Checking columns needed for Problem 1...")
    needed = ["tpep_pickup_datetime", "trip_distance", "mta_tax",
              "fare_amount", "tip_amount", "passenger_count"]
    absent = [c for c in needed if c not in df.columns]
    if absent:
        print(f"   ❌ Missing columns: {absent}")
        return False
    print("   ✅ All required columns present")
    return True


def main():
    """Main function to run the test."""
    print("NYC TLC Data Reading Diagnostic Tool")
    print("This will help identify why data reading is failing.\n")

    s3_path = sys.argv[1] if len(sys.argv) > 1 else os.getenv("TAXI_S3_PATH")
    if not s3_path:
        print("❌ S3 path not provided")
        print("Usage: python test_nyc_data_read.py s3://your-bucket/your-prefix")
        print("   or: export TAXI_S3_PATH=s3://your-bucket/your-prefix")
        return 1

    success = test_nyc_data_access(s3_path)

    print("\n" + "=" * 50)
    if success:
        print("✅ Diagnostic complete. Data access looks good.")
    else:
        print("❌ Problems found - see messages above.")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
