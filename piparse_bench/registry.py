"""Parameter registry: loads parameters.yaml and validates configs against it."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_REGISTRY: dict[str, Any] | None = None
_REGISTRY_PATH = Path(__file__).parent.parent / "configs" / "parameters.yaml"


def _load() -> dict[str, Any]:
    global _REGISTRY
    if _REGISTRY is None:
        with open(_REGISTRY_PATH) as f:
            _REGISTRY = yaml.safe_load(f)
    return _REGISTRY


def get_registry() -> dict[str, Any]:
    return _load()


def validate_worker_param(key: str, value: Any) -> list[str]:
    """Validate a single worker parameter. Returns list of errors."""
    reg = _load()
    params = reg.get("worker_params", {})
    if key not in params:
        return [f"Unknown worker parameter '{key}'. Valid: {list(params.keys())}"]

    spec = params[key]
    errors = []

    if "values" in spec and value not in spec["values"]:
        errors.append(f"Invalid value '{value}' for '{key}'. Valid: {spec['values']}")

    if "range" in spec:
        lo, hi = spec["range"]
        if not (lo <= value <= hi):
            errors.append(f"Value {value} for '{key}' out of range [{lo}, {hi}]")

    return errors


def validate_query_param(key: str, value: Any) -> list[str]:
    """Validate a single query-level parameter. Returns list of errors."""
    reg = _load()
    params = reg.get("query_params", {})
    if key not in params:
        return [f"Unknown query parameter '{key}'. Valid: {list(params.keys())}"]

    spec = params[key]
    errors = []

    if spec.get("type") == "map":
        return errors  # maps validated separately

    if "values" in spec and value not in spec["values"]:
        errors.append(f"Invalid value '{value}' for '{key}'. Valid: {spec['values']}")

    return errors


def flatten_worker_config(worker: dict[str, Any], prefix: str = "worker") -> dict[str, Any]:
    """Flatten nested worker config to dotted keys.

    e.g. {"query_engine": {"number_of_worker_threads": 16}}
    → {"worker.query_engine.number_of_worker_threads": 16}
    """
    result = {}
    for k, v in worker.items():
        full_key = f"{prefix}.{k}"
        if isinstance(v, dict):
            result.update(flatten_worker_config(v, full_key))
        else:
            result[full_key] = v
    return result
