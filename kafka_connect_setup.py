#!/usr/bin/env python3
"""
Idempotent script to setup Kafka Connect and MinIO for F1 analytics.

Waits for Kafka Connect REST API, creates MinIO bucket, and registers S3 sink connector.
"""

import argparse
import json
import sys
import time
from typing import Any, Dict

import boto3
from botocore.client import Config
from requests.exceptions import ConnectionError as RequestsConnectionError
import requests

CONNECTOR_NAME = "f1-s3-sink"
BUCKET_NAME = "f1-lakehouse"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "topics": "f1.timing.laps,f1.timing.position,f1.timing.intervals,f1.timing.session_result,f1.strategy.pit,f1.strategy.stints,f1.race.control,f1.env.weather,f1.ref.drivers",
        "s3.region": "us-east-1",
        "s3.bucket.name": BUCKET_NAME,
        "s3.endpoint": "http://minio:9000",
        "s3.credentials.provider.class": "org.apache.kafka.common.security.plain.PlainLoginModule",
        "aws.access.key.id": "minioadmin",
        "aws.secret.access.key": "minioadmin",
        "s3.part.size": "5242880",
        "flush.size": "1000",
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
        "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "partition.duration.ms": "3600000",
        "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
        "timestamp.extractor": "RecordField",
        "timestamp.field": "event_time",
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": "f1-dlq",
        "errors.deadletterqueue.topic.replication.factor": "1",
    },
}


def wait_for_connect(connect_url: str, timeout: int = 60) -> bool:
    """Wait for Kafka Connect REST API to become available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{connect_url}/connector-plugins", timeout=5)
            if resp.status_code == 200:
                print(f"✓ Kafka Connect is ready at {connect_url}")
                return True
        except RequestsConnectionError:
            pass
        print("Waiting for Kafka Connect...", end="\r")
        time.sleep(1)

    print(f"\n✗ Kafka Connect not ready after {timeout} seconds")
    return False


def create_minio_bucket(endpoint: str) -> bool:
    """Create MinIO bucket if it doesn't exist using boto3."""
    try:
        # Use path-style addressing for MinIO compatibility
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=Config(signature_version="s3v4"),
        )

        # Check if bucket exists
        try:
            client.head_bucket(Bucket=BUCKET_NAME)
            print(f"✓ MinIO bucket '{BUCKET_NAME}' already exists")
            return True
        except client.exceptions.ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                # Bucket doesn't exist, create it
                client.create_bucket(Bucket=BUCKET_NAME)
                print(f"✓ Created MinIO bucket '{BUCKET_NAME}'")
                return True
            else:
                raise

    except Exception as e:
        print(f"✗ Failed to create/check MinIO bucket: {e}")
        return False


def register_connector(connect_url: str) -> bool:
    """Register the S3 sink connector via PUT /connectors/{name}/config."""
    connector_url = f"{connect_url}/connectors/{CONNECTOR_NAME}/config"

    try:
        resp = requests.put(
            connector_url,
            json=CONNECTOR_CONFIG,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code in (200, 201, 409):  # 409 = already exists
            print(f"✓ Connector '{CONNECTOR_NAME}' registered")
            if resp.status_code == 200:
                return True
            # For 409, check status to verify it's running
            status_url = f"{connect_url}/connectors/{CONNECTOR_NAME}/status"
            try:
                status_resp = requests.get(status_url, timeout=10)
                if status_resp.status_code == 200:
                    state = status_resp.json().get("connector", {}).get("state", "")
                    if state.lower() == "running":
                        print(f"✓ Connector '{CONNECTOR_NAME}' is running")
                        return True
                    else:
                        print(f"⚠ Connector '{CONNECTOR_NAME}' exists but state is '{state}', expected 'running'")
                        return False
            except Exception:
                print("⚠ Could not verify connector status (may still be starting)")
                return True  # Assume success since it was created
        else:
            print(f"✗ Failed to register connector: HTTP {resp.status_code}")
            print(f"Response: {resp.text}")
            return False

    except Exception as e:
        print(f"✗ Error registering connector: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Setup Kafka Connect and MinIO for F1 analytics pipeline."
    )
    parser.add_argument(
        "--connect-url",
        type=str,
        default="http://localhost:8083",
        help="Kafka Connect REST API URL (default: http://localhost:8083)",
    )
    parser.add_argument(
        "--minio-endpoint",
        type=str,
        default="http://localhost:9000",
        help="MinIO endpoint URL (default: http://localhost:9000)",
    )
    args = parser.parse_args()

    print("Setting up F1 analytics pipeline components...")

    # Step 1: Wait for Kafka Connect
    if not wait_for_connect(args.connect_url):
        sys.exit(1)

    # Step 2: Create MinIO bucket
    if not create_minio_bucket(args.minio_endpoint):
        sys.exit(1)

    # Step 3: Register S3 sink connector
    if not register_connector(args.connect_url):
        sys.exit(1)

    print("\n✓ Setup completed successfully!")


if __name__ == "__main__":
    main()
