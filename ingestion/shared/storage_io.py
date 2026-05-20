from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit
from urllib.error import HTTPError
import urllib.request
from typing import Any

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


def _s3_client() -> Any:
    import boto3

    return boto3.client("s3")


def uri_exists(uri: str) -> bool:
    """Return whether a local or S3 URI exists."""
    if uri.startswith("s3://"):
        bucket, object_key = split_s3_uri(uri)
        try:
            _s3_client().head_object(Bucket=bucket, Key=object_key)
        except Exception:
            return False
        return True

    return Path(uri).exists()


def read_text(uri: str) -> str:
    """Read UTF-8 text from a local path or directly from S3."""
    if uri.startswith("s3://"):
        bucket, object_key = split_s3_uri(uri)
        response = _s3_client().get_object(Bucket=bucket, Key=object_key)
        return response["Body"].read().decode("utf-8")

    return Path(uri).read_text(encoding="utf-8")


def write_text(destination_uri: str, content: str) -> None:
    """Write UTF-8 text to a local path or directly to S3."""
    encoded = content.encode("utf-8")

    if destination_uri.startswith("s3://"):
        bucket, object_key = split_s3_uri(destination_uri)
        _s3_client().put_object(Bucket=bucket, Key=object_key, Body=encoded)
        return

    destination = Path(destination_uri)
    ensure_parent_dir(destination)
    destination.write_bytes(encoded)


def _normalize_http_metadata(response: Any) -> dict[str, Any]:
    content_length_raw = response.headers.get("Content-Length")
    content_length = None
    if content_length_raw is not None:
        try:
            content_length = int(content_length_raw)
        except ValueError:
            content_length = None

    return {
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
        "content_length": content_length,
        "content_type": response.headers.get("Content-Type"),
        "source_url": response.geturl(),
    }


def probe_http_resource(url: str) -> dict[str, Any] | None:
    """Return upstream metadata for one HTTP object, or None if it does not exist."""
    headers = {"User-Agent": HTTP_USER_AGENT}
    for method in ("HEAD", "GET"):
        request = urllib.request.Request(url, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request) as response:
                return _normalize_http_metadata(response)
        except HTTPError as exc:
            if exc.code == 404:
                return None
            if method == "HEAD" and exc.code in {403, 405}:
                continue
            raise
    return None


def write_json(destination_uri: str, payload: dict[str, Any]) -> None:
    """Serialize one JSON payload to a local path or directly to S3."""
    write_text(destination_uri, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def download_file(url: str, destination_uri: str) -> dict[str, Any]:
    """Download a remote file to a local path or directly to S3."""
    request = urllib.request.Request(url, headers={"User-Agent": HTTP_USER_AGENT})

    with urllib.request.urlopen(request) as response:
        metadata = _normalize_http_metadata(response)
        if destination_uri.startswith("s3://"):
            bucket, object_key = split_s3_uri(destination_uri)
            _s3_client().upload_fileobj(response, bucket, object_key)
            return metadata

        destination = Path(destination_uri)
        ensure_parent_dir(destination)
        with destination.open("wb") as handle:
            while True:
                chunk = response.read(8 * 1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
        return metadata
