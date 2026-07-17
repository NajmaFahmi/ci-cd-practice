"""
Google Cloud Storage operations for the raw data landing zone.

Demonstrates the core object-storage primitives: upload, list with prefix
filtering, metadata inspection, and download. Object keys follow a
hive-style partition convention so downstream engines can prune partitions.
"""
from __future__ import annotations

from pathlib import Path

from google.cloud import storage

PROJECT_ID = "najma-de-learning"
BUCKET_NAME = "retailco-raw-najma"


def get_bucket() -> storage.Bucket:
    client = storage.Client(project=PROJECT_ID)
    return client.bucket(BUCKET_NAME)


def upload(local_path: Path, object_key: str) -> None:
    """Upload a local file to the bucket under the given object key."""
    blob = get_bucket().blob(object_key)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path.name} -> gs://{BUCKET_NAME}/{object_key}")


def list_objects(prefix: str = "") -> list[str]:
    """List object keys matching a prefix.

    GCS has no real directories: this is a prefix scan over a flat keyspace,
    which is why 'listing a folder' is really 'filtering keys by string prefix'.
    """
    blobs = get_bucket().list_blobs(prefix=prefix)
    return [blob.name for blob in blobs]


def describe(object_key: str) -> None:
    """Print the metadata GCS stores alongside the object bytes."""
    blob = get_bucket().get_blob(object_key)
    if blob is None:
        print(f"Object not found: {object_key}")
        return

    print(f"key           : {blob.name}")
    print(f"size          : {blob.size} bytes")
    print(f"storage class : {blob.storage_class}")
    print(f"created       : {blob.time_created}")
    print(f"md5           : {blob.md5_hash}")


def download(object_key: str, local_path: Path) -> None:
    """Download an object to the local filesystem."""
    blob = get_bucket().blob(object_key)
    blob.download_to_filename(local_path)
    print(f"Downloaded gs://{BUCKET_NAME}/{object_key} -> {local_path}")


def list_partitions(dataset_prefix: str) -> list[str]:
    """Return the distinct partition paths under a dataset prefix.
    
    Example: ['nyc_taxi/year=2026/month=06/', 'nyc_taxi/year=2026/month=07/']
    """
    bucket = get_bucket()
    iterator = bucket.list_blobs(prefix=dataset_prefix, delimiter='/')
    page = next(iterator.pages)
    return page.prefixes



if __name__ == "__main__":
    here = Path(__file__).parent
    key = "nyc_taxi/year=2026/month=07/sample_trips_python.csv"

    upload(here / "sample_trips.csv", key)

    print("\nObjects under nyc_taxi/:")
    for name in list_objects("nyc_taxi/"):
        print(f"  {name}")

    print("\nMetadata:")
    describe(key)

    download(key, here / "downloaded_trips.csv")

    print("\nPartitions under nyc_taxi/:")
    for partition in list_partitions("nyc_taxi/"):
        print(f"  {partition}")