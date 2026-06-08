"""
Tear down the test Aurora cluster created by ``provision_test_cluster.py``.

Deletes the writer instance first (required), then the cluster (with
SkipFinalSnapshot), then the master secret with no recovery window.
Idempotent: missing resources are skipped.
"""
from __future__ import annotations

import sys
import time

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
CLUSTER_ID = "aurora-data-api-test-dev"
SECRET_NAME = "aurora-data-api-test-dev-master"


def delete_writer(rds) -> None:
    writer_id = f"{CLUSTER_ID}-writer"
    try:
        rds.delete_db_instance(
            DBInstanceIdentifier=writer_id,
            SkipFinalSnapshot=True,
        )
        print(f"  delete initiated: {writer_id}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "DBInstanceNotFound":
            print(f"  writer not found: {writer_id} (skipping)")
            return
        raise

    # Wait until the instance is gone before deleting the cluster.
    print(f"  waiting for {writer_id} deletion...")
    deadline = time.time() + 600
    while time.time() < deadline:
        try:
            resp = rds.describe_db_instances(DBInstanceIdentifier=writer_id)
            status = resp["DBInstances"][0]["DBInstanceStatus"]
            print(f"    status: {status}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "DBInstanceNotFound":
                print(f"    deleted.")
                return
            raise
        time.sleep(15)
    raise TimeoutError(f"Writer {writer_id} did not delete in 600s")


def delete_cluster(rds) -> None:
    try:
        rds.delete_db_cluster(
            DBClusterIdentifier=CLUSTER_ID,
            SkipFinalSnapshot=True,
        )
        print(f"  delete initiated: {CLUSTER_ID}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "DBClusterNotFoundFault":
            print(f"  cluster not found: {CLUSTER_ID} (skipping)")
            return
        raise


def delete_secret(sm) -> None:
    try:
        sm.delete_secret(
            SecretId=SECRET_NAME,
            ForceDeleteWithoutRecovery=True,
        )
        print(f"  delete initiated: {SECRET_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"  secret not found: {SECRET_NAME} (skipping)")
            return
        raise


def main() -> int:
    session = boto3.Session(region_name=REGION)
    rds = session.client("rds")
    sm = session.client("secretsmanager")

    print("[1/3] deleting writer instance...")
    delete_writer(rds)

    print("[2/3] deleting cluster...")
    delete_cluster(rds)

    print("[3/3] deleting secret...")
    delete_secret(sm)

    print("teardown done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
