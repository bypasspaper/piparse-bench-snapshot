"""Validation rules for experiment definitions."""

from __future__ import annotations

from typing import Any

from .models import (
    ExperimentFile,
    ExplicitSeries,
    ParameterSeries,
    Variant,
)
from .registry import get_registry
from .resolver import _set_nested


def validate_baseline(experiment: ExperimentFile) -> list[str]:
    """Check that every variant with a series axis has exactly one baseline."""
    errors = []
    for variant in experiment.variants:
        series = variant.axes.series
        if series is None:
            continue

        if isinstance(series, ParameterSeries):
            if series.baseline is None:
                errors.append(
                    f"Variant '{variant.name}': parameter series '{series.label}' "
                    f"must specify a baseline value. Valid: {series.values}"
                )
            elif series.baseline not in series.values:
                errors.append(
                    f"Variant '{variant.name}': baseline '{series.baseline}' "
                    f"not in series values {series.values}"
                )

        elif isinstance(series, ExplicitSeries):
            baselines = [e for e in series.explicit if e.baseline]
            if len(baselines) == 0:
                names = [e.name for e in series.explicit]
                errors.append(
                    f"Variant '{variant.name}': explicit series '{series.label}' "
                    f"must have exactly one entry with baseline: true. "
                    f"Series entries: {names}"
                )
            elif len(baselines) > 1:
                names = [e.name for e in baselines]
                errors.append(
                    f"Variant '{variant.name}': multiple baselines in series "
                    f"'{series.label}': {names}. Exactly one is required."
                )

    return errors


def validate_query_refs(variant: Variant) -> list[str]:
    """Check that exactly one of query/queries is specified."""
    errors = []
    if variant.query and variant.queries:
        errors.append(
            f"Variant '{variant.name}': specify either 'query' or 'queries', not both"
        )
    if not variant.query and not variant.queries:
        errors.append(
            f"Variant '{variant.name}': must specify either 'query' or 'queries'"
        )
    return errors


def _get_nested(d: dict[str, Any], dotted_key: str) -> Any:
    """Get a value from a nested dict using a dotted key path."""
    parts = dotted_key.split(".")
    current = d
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def validate_dependencies(experiment: ExperimentFile) -> list[str]:
    """Check dependency rules from the parameter registry.

    Rules are defined in configs/parameters.yaml under 'dependencies'.
    Each rule has:
      - if: {param: [values]}  — condition to check
      - requires: {param: value} or [feature] — what must be present
      - note: optional explanation
    """
    registry = get_registry()
    dep_rules = registry.get("dependencies", [])
    if not dep_rules:
        return []

    errors = []
    defaults = experiment.defaults.model_dump()

    for variant in experiment.variants:
        # Build the effective config for this variant (without axis expansion)
        config = _merge_defaults_and_overrides(defaults, variant)

        for rule in dep_rules:
            condition = rule.get("if", {})
            requires = rule.get("requires", {})
            note = rule.get("note", "")

            if not _condition_matches(config, condition):
                continue

            # Condition matches — check requirement
            req_errors = _check_requirement(config, requires, rule)
            for err in req_errors:
                msg = f"Variant '{variant.name}': {err}"
                if note:
                    msg += f" ({note})"
                errors.append(msg)

    return errors


def _merge_defaults_and_overrides(defaults: dict, variant: Variant) -> dict:
    """Build a flat config dict from defaults + variant overrides (no axis expansion)."""
    import copy
    config = copy.deepcopy(defaults)
    for key, value in variant.overrides.items():
        if "." in key:
            _set_nested(config, key, value)
        elif isinstance(value, dict) and key in config and isinstance(config[key], dict):
            config[key].update(value)
        else:
            config[key] = value
    return config


def _condition_matches(config: dict, condition: dict) -> bool:
    """Check if the condition dict matches the config."""
    for param, expected_values in condition.items():
        actual = _get_nested(config, param) or _get_nested(config, f"query_config.{param}")
        if actual is None:
            return False
        if isinstance(expected_values, list):
            if actual not in expected_values:
                return False
        elif isinstance(expected_values, bool):
            if actual != expected_values:
                return False
        else:
            if actual != expected_values:
                return False
    return True


def _check_requirement(config: dict, requires: Any, rule: dict) -> list[str]:
    """Check if the requirement is met. Returns list of errors."""
    errors = []

    if isinstance(requires, list):
        # requires: [feature_name] — check the feature is configured
        for feature in requires:
            val = _get_nested(config, feature)
            if val is None:
                mode = rule.get("mode", "")
                errors.append(
                    f"requires '{feature}' to be configured"
                    + (f" (mode: {mode})" if mode else "")
                )

    elif isinstance(requires, dict):
        # requires: {param: value} — check specific parameter values
        for param, expected in requires.items():
            actual = _get_nested(config, param)
            if actual is None:
                actual = _get_nested(config, f"query_config.{param}")
            if isinstance(expected, list):
                if actual not in expected:
                    errors.append(
                        f"requires '{param}' to be one of {expected}, got '{actual}'"
                    )
            elif actual != expected:
                errors.append(
                    f"requires '{param}' = '{expected}', got '{actual}'"
                )

    return errors


def validate_experiment(experiment: ExperimentFile) -> list[str]:
    """Run all validation checks. Returns list of errors (empty = valid)."""
    errors = []
    errors.extend(validate_baseline(experiment))
    for variant in experiment.variants:
        errors.extend(validate_query_refs(variant))
    errors.extend(validate_dependencies(experiment))
    return errors
