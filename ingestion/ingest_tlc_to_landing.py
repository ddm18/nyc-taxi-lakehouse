#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.landing import ingest_to_landing
from ingestion.shared.dto import DatasetMonthDTO, LandingIngestionRequestDTO


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the Landing ingestion entrypoint."""
    parser = argparse.ArgumentParser(
        description=(
            "Download one TLC dataset-month into a deterministic Landing location."
        )
    )
    parser.add_argument("--service", required=True, choices=("yellow", "green"))
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    parser.add_argument(
        "--landing-root",
        required=True,
        help="Root destination, for example /tmp/nyc-lakehouse or s3://bucket/prefix",
    )
    return parser.parse_args()


def main() -> int:
    """Run the Landing ingestion CLI workflow and print its summary."""
    args = parse_args()
    request = LandingIngestionRequestDTO(
        dataset=DatasetMonthDTO(
            service=args.service,
            year=args.year,
            month=args.month,
        ),
        landing_root_uri=args.landing_root,
    )
    summary = ingest_to_landing(request)
    print(json.dumps(asdict(summary), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
