#!/usr/bin/env python3
"""
Dask Installation Test Script

This script tests the Dask installation to ensure everything is working correctly.
Run this after completing the environment setup to verify your installation.

Usage:
    python dask_installation_test.py                    # local cluster
    python dask_installation_test.py tcp://IP:8786      # your EC2 cluster
"""

import sys

import dask
import dask.array as da
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster


def test_dask_installation(scheduler=None):
    """Test Dask installation and basic functionality."""

    print("Testing Dask Installation...")
    print("=" * 50)

    try:
        # Test 1: Start a cluster (or connect to one)
        print("1. Creating Dask client...")
        if scheduler:
            client = Client(scheduler, timeout=30)
        else:
            client = Client(LocalCluster(n_workers=2, threads_per_worker=2))
        info = client.scheduler_info()
        print("   ✅ Dask client created successfully")
        print(f"   ✅ Dask version: {dask.__version__}")
        print(f"   ✅ Workers connected: {len(info['workers'])}")
        print(f"   ✅ Dashboard: {client.dashboard_link}")

        # Test 2: Basic array operations
        print("\n2. Testing basic array operations...")
        x = da.ones((10000, 10000), chunks=(1000, 1000))
        total = int(x.sum().compute())
        assert total == 100_000_000, total
        print(f"   ✅ Distributed array sum correct: {total:,}")

        # Test 3: DataFrame operations
        print("\n3. Testing DataFrame operations...")
        df = dd.from_pandas(pd.DataFrame({"id": range(1000)}), npartitions=4)
        count = len(df)
        print(f"   ✅ Created DataFrame with {count} rows")

        # Test 4: Data transformations
        print("\n4. Testing data transformations...")
        transformed = df[df.id % 2 == 0].assign(doubled=lambda d: d.id * 2)
        transformed_count = len(transformed)
        print(f"   ✅ Data transformation successful: {transformed_count} rows after filtering")

        # Test 5: S3 support
        print("\n5. Testing S3 support (s3fs)...")
        import s3fs  # noqa: F401
        import pyarrow  # noqa: F401
        print("   ✅ s3fs and pyarrow import successfully")

        client.close()
        print("\nAll tests passed! Dask installation is working correctly.")
        print("\nYou can now proceed with the lab exercises.")
        return True

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        print("\nPlease check your installation and try again.")
        return False


if __name__ == "__main__":
    success = test_dask_installation(sys.argv[1] if len(sys.argv) > 1 else None)
    sys.exit(0 if success else 1)
