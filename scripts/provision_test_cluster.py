"""
Provision a small Aurora Postgres Serverless v2 cluster + writer instance
for running the SQLAlchemy compliance suite against the Data API.

Idempotent: re-running checks whether the cluster already exists and just
re-emits the ARNs. Pairs with ``teardown_test_cluster.py``.

Outputs ``test/.env`` with CLUSTER_ARN, SECRET_ARN, DATABASE — picked up
by the conftest at test time.
"""
from __future__ import annotations

import json
import os
import secrets
import string
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
CLUSTER_ID = "aurora-data-api-test-dev"
DATABASE_NAME = "ada_test"
SECRET_NAME = "aurora-data-api-test-dev-master"
ENGINE_VERSION = "16.6"   # Aurora PG 16 has the broadest Data API support
# MinCapacity=0 is required to use SecondsUntilAutoPause (AWS constraint:
# auto-pause only triggers when the cluster can scale all the way down).
MIN_ACU = 0
MAX_ACU = 2.0
SECONDS_UNTIL_AUTO_PAUSE = 300

TAGS = [
    {"Key": "Project", "Value": "aurora-data-api-test"},
    {"Key": "Purpose", "Value": "sqlalchemy-compliance-suite"},
]

# Output location (in the test/.env of the worktree this script lives in).
ENV_PATH = Path(__file__).resolve().parent.parent / "test" / ".env"


def _gen_password() -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(40))


def ensure_secret(sm) -> tuple[str, str]:
    """Create or fetch the master credentials secret. Returns (arn, password)."""
    try:
        resp = sm.describe_secret(SecretId=SECRET_NAME)
        secret_arn = resp["ARN"]
        val = sm.get_secret_value(SecretId=SECRET_NAME)
        password = json.loads(val["SecretString"])["password"]
        print(f"  secret exists: {secret_arn}")
        return secret_arn, password
    except sm.exceptions.ResourceNotFoundException:
        password = _gen_password()
        resp = sm.create_secret(
            Name=SECRET_NAME,
            Description="Master creds for aurora-data-api test cluster",
            SecretString=json.dumps({"username": "postgres", "password": password}),
            Tags=TAGS,
        )
        secret_arn = resp["ARN"]
        print(f"  secret created: {secret_arn}")
        return secret_arn, password


def ensure_cluster(rds, password: str) -> str:
    """Create or fetch the cluster. Returns the cluster ARN."""
    try:
        resp = rds.describe_db_clusters(DBClusterIdentifier=CLUSTER_ID)
        cluster_arn = resp["DBClusters"][0]["DBClusterArn"]
        status = resp["DBClusters"][0]["Status"]
        print(f"  cluster exists: {cluster_arn} (status={status})")
        return cluster_arn
    except ClientError as e:
        if e.response["Error"]["Code"] != "DBClusterNotFoundFault":
            raise

    print(f"  creating cluster {CLUSTER_ID}...")
    resp = rds.create_db_cluster(
        DBClusterIdentifier=CLUSTER_ID,
        Engine="aurora-postgresql",
        EngineVersion=ENGINE_VERSION,
        MasterUsername="postgres",
        MasterUserPassword=password,
        DatabaseName=DATABASE_NAME,
        EnableHttpEndpoint=True,         # Data API
        ServerlessV2ScalingConfiguration={
            "MinCapacity": MIN_ACU,
            "MaxCapacity": MAX_ACU,
            "SecondsUntilAutoPause": SECONDS_UNTIL_AUTO_PAUSE,
        },
        StorageEncrypted=True,
        Tags=TAGS,
        DeletionProtection=False,
        BackupRetentionPeriod=1,
    )
    cluster_arn = resp["DBCluster"]["DBClusterArn"]
    print(f"  cluster created: {cluster_arn}")
    return cluster_arn


def ensure_writer_instance(rds) -> str:
    """Create or fetch the writer instance for the cluster."""
    writer_id = f"{CLUSTER_ID}-writer"
    try:
        resp = rds.describe_db_instances(DBInstanceIdentifier=writer_id)
        status = resp["DBInstances"][0]["DBInstanceStatus"]
        print(f"  writer exists: {writer_id} (status={status})")
        return writer_id
    except ClientError as e:
        if e.response["Error"]["Code"] != "DBInstanceNotFound":
            raise

    print(f"  creating writer instance {writer_id}...")
    rds.create_db_instance(
        DBInstanceIdentifier=writer_id,
        DBClusterIdentifier=CLUSTER_ID,
        DBInstanceClass="db.serverless",
        Engine="aurora-postgresql",
        Tags=TAGS,
    )
    return writer_id


def wait_for_cluster_available(rds, cluster_arn: str, timeout_s: int = 900) -> None:
    print(f"  waiting for cluster to be available (timeout {timeout_s}s)...")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        resp = rds.describe_db_clusters(DBClusterIdentifier=CLUSTER_ID)
        status = resp["DBClusters"][0]["Status"]
        print(f"    status: {status}")
        if status == "available":
            return
        time.sleep(15)
    raise TimeoutError(f"Cluster {CLUSTER_ID} did not reach 'available' in {timeout_s}s")


def write_env(cluster_arn: str, secret_arn: str) -> None:
    ENV_PATH.parent.mkdir(parents=True, exist_ok=True)
    ENV_PATH.write_text(
        "# Generated by scripts/provision_test_cluster.py. Do not commit.\n"
        f"AURORA_CLUSTER_ARN={cluster_arn}\n"
        f"AURORA_SECRET_ARN={secret_arn}\n"
        f"AURORA_DATABASE={DATABASE_NAME}\n"
        f"AWS_REGION={REGION}\n"
    )
    print(f"  wrote {ENV_PATH}")


def main() -> int:
    session = boto3.Session(region_name=REGION)
    rds = session.client("rds")
    sm = session.client("secretsmanager")

    print(f"[1/4] ensuring secret in Secrets Manager...")
    secret_arn, password = ensure_secret(sm)

    print(f"[2/4] ensuring cluster...")
    cluster_arn = ensure_cluster(rds, password)

    print(f"[3/4] ensuring writer instance...")
    ensure_writer_instance(rds)

    print(f"[4/4] waiting for cluster availability...")
    wait_for_cluster_available(rds, cluster_arn)

    write_env(cluster_arn, secret_arn)
    print("done.")
    print(f"  CLUSTER_ARN={cluster_arn}")
    print(f"  SECRET_ARN={secret_arn}")
    print(f"  DATABASE={DATABASE_NAME}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
