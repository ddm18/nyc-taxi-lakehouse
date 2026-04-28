from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import TypedDict, cast

from ingestion.shared.dto import DatasetMonthDTO

SOURCE_DIR = Path(__file__).resolve().parents[1] / "sources"


class AccessConfig(TypedDict):
    base_url: str


class ObjectsConfig(TypedDict):
    request_path_pattern: str


class IngestionConfig(TypedDict):
    landing_path_pattern: str
    landing_file_name: str


class SourceConfig(TypedDict):
    access: AccessConfig
    objects: ObjectsConfig
    ingestion: IngestionConfig


def _parse_scalar(value: str) -> Any:
    normalized = value.strip()
    if normalized in {"true", "false"}:
        return normalized == "true"
    if normalized.isdigit():
        return int(normalized)
    return normalized.strip("'\"")


def _load_simple_yaml(source_text: str) -> dict[str, Any]:
    lines = [
        line.rstrip()
        for line in source_text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    index = 0

    def parse_list(indent: int) -> list[Any]:
        nonlocal index
        items: list[Any] = []
        while index < len(lines):
            line = lines[index]
            current_indent = len(line) - len(line.lstrip(" "))
            stripped = line.strip()
            if current_indent < indent or not stripped.startswith("- "):
                break

            item_value = stripped[2:].strip()
            index += 1
            if item_value:
                items.append(_parse_scalar(item_value))
                continue

            if index < len(lines):
                next_indent = len(lines[index]) - len(lines[index].lstrip(" "))
                if next_indent > current_indent:
                    items.append(parse_mapping(next_indent))
                    continue

            items.append(None)
        return items

    def parse_mapping(indent: int) -> dict[str, Any]:
        nonlocal index
        mapping: dict[str, Any] = {}
        while index < len(lines):
            line = lines[index]
            current_indent = len(line) - len(line.lstrip(" "))
            stripped = line.strip()
            if current_indent < indent or stripped.startswith("- "):
                break
            if current_indent != indent:
                raise ValueError(f"Unexpected indentation in source config: {line}")

            key, separator, raw_value = stripped.partition(":")
            if not separator:
                raise ValueError(f"Expected key/value mapping in source config: {line}")

            index += 1
            value = raw_value.strip()
            if value:
                mapping[key] = _parse_scalar(value)
                continue

            if index >= len(lines):
                mapping[key] = {}
                continue

            next_line = lines[index]
            next_indent = len(next_line) - len(next_line.lstrip(" "))
            next_stripped = next_line.strip()
            if next_indent <= current_indent:
                mapping[key] = {}
            elif next_stripped.startswith("- "):
                mapping[key] = parse_list(next_indent)
            else:
                mapping[key] = parse_mapping(next_indent)
        return mapping

    return parse_mapping(0)


def load_source_config(service: str) -> SourceConfig:
    """Load the YAML source descriptor for one TLC service."""
    source_path = SOURCE_DIR / f"{service}_source.yaml"
    source_text = source_path.read_text(encoding="utf-8")

    try:
        import yaml  # pyright: ignore[reportMissingModuleSource]

        raw_config = yaml.safe_load(source_text)
    except ModuleNotFoundError:
        raw_config = _load_simple_yaml(source_text)

    if not isinstance(raw_config, dict):
        raise ValueError(f"Expected mapping in {source_path}")
    return cast(SourceConfig, raw_config)


def render_pattern(pattern: str, dataset: DatasetMonthDTO) -> str:
    """Resolve service/year/month placeholders inside a config pattern."""
    rendered = pattern.replace("{service}", dataset.service)
    rendered = rendered.replace("{YYYY}", f"{dataset.year:04d}")
    rendered = rendered.replace("{MM}", f"{dataset.month:02d}")
    rendered = rendered.replace("YYYY", f"{dataset.year:04d}")
    rendered = rendered.replace("MM", f"{dataset.month:02d}")
    return rendered


def build_source_url(config: SourceConfig, dataset: DatasetMonthDTO) -> str:
    """Build the public TLC download URL for a dataset-month."""
    request_path = render_pattern(config["objects"]["request_path_pattern"], dataset)
    normalized_base = config["access"]["base_url"].rstrip("/")
    normalized_request = request_path.lstrip("/")

    if normalized_base.endswith("/trip-data") and normalized_request.startswith(
        "trip-data/"
    ):
        normalized_request = normalized_request.removeprefix("trip-data/")

    return f"{normalized_base}/{normalized_request}"


def build_landing_relative_path(config: SourceConfig, dataset: DatasetMonthDTO) -> Path:
    """Build the deterministic landing-relative path for a dataset-month."""
    landing_dir = render_pattern(config["ingestion"]["landing_path_pattern"], dataset)
    landing_file = render_pattern(config["ingestion"]["landing_file_name"], dataset)
    return Path(landing_dir) / landing_file


def build_landing_object_uri(
    config: SourceConfig,
    dataset: DatasetMonthDTO,
    landing_root_uri: str,
) -> str:
    """Build the full landing object URI for a dataset-month."""
    relative_path = build_landing_relative_path(config, dataset).as_posix()
    normalized_root = landing_root_uri.rstrip("/")

    if normalized_root.startswith("s3://"):
        return f"{normalized_root}/{relative_path}"

    return str((Path(normalized_root).expanduser().resolve() / relative_path))
