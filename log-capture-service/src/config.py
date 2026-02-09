"""Configuration loading from CLI args, env vars, and optional YAML file."""

import os
import logging
from dataclasses import dataclass, field

import yaml

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FilterRule:
    pattern: str   # regex
    action: str    # "include" or "exclude"


@dataclass(frozen=True)
class TagRule:
    name: str      # tag name e.g. "critical"
    pattern: str   # regex to match
    field: str     # "message", "level", "service", or "raw"


@dataclass(frozen=True)
class Config:
    log_files: list[str] = field(default_factory=list)
    output_dir: str = "collected_logs/"
    batch_size: int = 50
    flush_interval: float = 5.0
    registry_file: str = "collected_logs/.registry.json"
    filter_rules: list[FilterRule] = field(default_factory=list)
    tag_rules: list[TagRule] = field(default_factory=list)


def load_yaml_config(path: str | None) -> dict:
    """Load filter/tag rules from a YAML file. Returns empty dict if no path."""
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        logger.info("Loaded YAML config from %s", path)
        return data
    except FileNotFoundError:
        logger.warning("Config file %s not found, using defaults", path)
        return {}


def load_config(cli_args, yaml_data: dict) -> Config:
    """Build Config from CLI args, env vars, and parsed YAML data."""
    filter_rules = [
        FilterRule(pattern=r["pattern"], action=r["action"])
        for r in yaml_data.get("filters", [])
    ]
    tag_rules = [
        TagRule(name=r["name"], pattern=r["pattern"], field=r["field"])
        for r in yaml_data.get("tags", [])
    ]

    return Config(
        log_files=cli_args.log_files,
        output_dir=cli_args.output_dir,
        batch_size=int(os.environ.get("BATCH_SIZE", "50")),
        flush_interval=float(os.environ.get("FLUSH_INTERVAL", "5.0")),
        registry_file=os.environ.get("REGISTRY_FILE", "collected_logs/.registry.json"),
        filter_rules=filter_rules,
        tag_rules=tag_rules,
    )
