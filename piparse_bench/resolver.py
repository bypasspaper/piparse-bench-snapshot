"""Load experiment YAML, resolve query catalog references, merge defaults with overrides."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from .models import (
    Axes,
    CartesianAxis,
    ExperimentFile,
    ExplicitSeries,
    ParameterSeries,
    QueryCatalogEntry,
    QueryCatalogFile,
    SimpleAxis,
    Variant,
)

QUERIES_DIR = Path(__file__).parent.parent / "queries"


def load_experiment(path: str | Path) -> ExperimentFile:
    """Load and validate an experiment YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return ExperimentFile.model_validate(raw)


def load_query(ref: str) -> QueryCatalogEntry:
    """Load a query catalog entry by reference (e.g. 'nexmark/bid_star')."""
    query_path = QUERIES_DIR / f"{ref}.yaml"
    if not query_path.exists():
        raise FileNotFoundError(
            f"Query catalog entry not found: {query_path}\n"
            f"Available: {[str(p.relative_to(QUERIES_DIR).with_suffix('')) for p in QUERIES_DIR.rglob('*.yaml')]}"
        )
    with open(query_path) as f:
        raw = yaml.safe_load(f)
    return QueryCatalogFile.model_validate(raw).query


def compute_input_rows(config) -> int | None:
    """Logical input rows a run ingests: catalog DataEntry.rows summed over the query's
    sources (for the run's data_size, falling back to the first entry like the renderer's
    file resolution), times generator.repeat (File replay / TCP loop — NB: io_uring file
    sources IGNORE NES_FILE_REPEAT, so experiments using them must keep repeat honest at 1).
    Returns None when any source's data entry lacks a `rows:` count — the derived
    throughput_mrows_s is then left NULL rather than guessed. Rows are format-invariant
    (the same logical stream as .csv or .nes has the SAME count), which is the point:
    Mrows/s comparisons charge a format its byte footprint, MB/s(own input) does not."""
    query = load_query(config.query_ref)
    total = 0
    for src in query.sources:
        entry = src.data.get(config.data_size) or (next(iter(src.data.values())) if src.data else None)
        if entry is None or entry.rows is None:
            return None
        total += entry.rows * max(1, src.instances)
    repeat = int((config.generator or {}).get("repeat", 1) or 1)
    return total * max(1, repeat)


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge override into base. Override values win."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _set_nested(d: dict[str, Any], dotted_key: str, value: Any) -> None:
    """Set a value in a nested dict using a dotted key path.

    e.g. _set_nested(d, "query_config.input_formatter.type", "CSV")
    """
    parts = dotted_key.split(".")
    current = d
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def resolve_variant(defaults: dict[str, Any], variant: Variant) -> dict[str, Any]:
    """Merge experiment defaults with variant-level overrides into a resolved config dict."""
    resolved = copy.deepcopy(defaults)

    # Apply variant.overrides (flat dotted keys or nested dicts)
    for key, value in variant.overrides.items():
        if "." in key:
            _set_nested(resolved, key, value)
        elif isinstance(value, dict) and key in resolved and isinstance(resolved[key], dict):
            resolved[key] = deep_merge(resolved[key], value)
        else:
            resolved[key] = value

    return resolved


def resolve_series_config(
    base_config: dict[str, Any],
    axes: Axes,
    series_entry: dict[str, Any],
) -> dict[str, Any]:
    """Apply series-level overrides to a base config.

    series_entry is either:
    - {"name": X, "overrides": {...}} for explicit series
    - {"name": X, "parameter": "p", "value": V} for parameter series
    """
    config = copy.deepcopy(base_config)

    if "overrides" in series_entry:
        for key, value in series_entry["overrides"].items():
            if "." in key:
                _set_nested(config, key, value)
            elif isinstance(value, dict) and key in config and isinstance(config[key], dict):
                config[key] = deep_merge(config[key], value)
            else:
                config[key] = value

    if "parameter" in series_entry:
        _set_nested(config, series_entry["parameter"], series_entry["value"])

    if "system" in series_entry and series_entry["system"]:
        config["system"] = series_entry["system"]

    return config
