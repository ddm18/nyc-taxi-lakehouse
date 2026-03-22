from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit
import urllib.request

import boto3

HTTP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)


def ensure_parent_dir(path: Path) -> None:
    """Create the parent directory for a file path if it does not exist."""
    path.parent.mkdir(parents=True, exist_ok=True)


def split_s3_uri(uri: str) -> tuple[str, str]:
    """Split an S3 URI into bucket and object key."""
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Unsupported S3 URI: {uri}")

    object_key = parsed.path.lstrip("/")
    if not object_key:
        raise ValueError(f"S3 URI must include an object key: {uri}")

    return parsed.netloc, object_key


def download_file(url: str, destination_uri: str) -> None:
    """Download a remote file to a local path or directly to S3."""
    request = urllib.request.Request(url, headers={"User-Agent": HTTP_USER_AGENT})

    with urllib.request.urlopen(request) as response:
        if destination_uri.startswith("s3://"):
            bucket, object_key = split_s3_uri(destination_uri)
            boto3.client("s3").upload_fileobj(response, bucket, object_key)
            return

        destination = Path(destination_uri)
        ensure_parent_dir(destination)
        with destination.open("wb") as handle:
            while True:
                chunk = response.read(8 * 1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
