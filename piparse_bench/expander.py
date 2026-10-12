"""Expand variant axes into a flat list of RunConfigs via cartesian product."""

from __future__ import annotations

import copy
import itertools
from typing import Any

from .models import (
    Axes,
    CartesianAxis,
    ExperimentFile,
    ExplicitAxis,
    ExplicitSeries,
    ParameterSeries,
    RunConfig,
    SimpleAxis,
    Variant,
)
from .resolver import resolve_series_config, resolve_variant, _set_nested


def _expand_x_axis(axes: Axes) -> list[tuple[str, str, dict[str, Any]]]:
    """Expand the X axis into (x_param, x_value_label, overrides) tuples."""
    x = axes.x

    if isinstance(x, SimpleAxis):
        return [
            (x.parameter, str(v), {x.parameter: v})
            for v in x.values
        ]

    if isinstance(x, CartesianAxis):
        params = list(x.cartesian.keys())
        value_lists = list(x.cartesian.values())
        results = []
        for combo in itertools.product(*value_lists):
            overrides = dict(zip(params, combo))
            label = " / ".join(str(v) for v in combo)
            param_label = " × ".join(p.split(".")[-1] for p in params)
            results.append((param_label, label, overrides))
        return results

    if isinstance(x, ExplicitAxis):
        return [
            (x.label, entry.name, entry.overrides)
            for entry in x.explicit
        ]

    raise ValueError(f"Unknown X axis type: {type(x)}")


def _expand_series(axes: Axes) -> list[dict[str, Any]]:
    """Expand the series axis into a list of series entry dicts."""
    series = axes.series

    if series is None:
        return [{"name": "default"}]

    if isinstance(series, ParameterSeries):
        return [
            {"name": str(v), "parameter": series.parameter, "value": v}
            for v in series.values
        ]

    if isinstance(series, ExplicitSeries):
        return [
            {
                "name": entry.name,
                "overrides": entry.overrides,
                "system": entry.system,
                "baseline": entry.baseline,
            }
            for entry in series.explicit
        ]

    raise ValueError(f"Unknown series type: {type(series)}")


def expand_variant(
    experiment: ExperimentFile,
    variant: Variant,
) -> list[RunConfig]:
    """Expand a single variant into a flat list of RunConfigs."""
    defaults_dict = experiment.defaults.model_dump()
    base_config = resolve_variant(defaults_dict, variant)
    repetitions = experiment.experiment.repetitions
    query_refs = variant.get_query_refs()

    x_points = _expand_x_axis(variant.axes)
    series_entries = _expand_series(variant.axes)

    configs: list[RunConfig] = []

    for series_entry in series_entries:
        series_config = resolve_series_config(base_config, variant.axes, series_entry)

        for x_param, x_value, x_overrides in x_points:
            point_config = copy.deepcopy(series_config)
            for key, value in x_overrides.items():
                _set_nested(point_config, key, value)

            for query_ref in query_refs:
                for rep in range(1, repetitions + 1):
                    rc = RunConfig(
                        experiment=experiment.experiment.name,
                        variant=variant.name,
                        series=series_entry["name"],
                        repetition=rep,
                        x_param=x_param,
                        x_value=x_value,
                        system=point_config.get("system", "nebulastream"),
                        branch=point_config.get("branch"),
                        mode=point_config.get("mode", "systest"),
                        build=point_config.get("build", "native"),
                        data_size=point_config.get("data_size", "large"),
                        topology=point_config.get("topology", "single-node"),
                        worker_flags=point_config.get("worker", {}),
                        query_config=point_config.get("query_config", {}),
                        query_ref=query_ref,
                        generator=point_config.get("generator"),
                        duration_seconds=point_config.get("duration_seconds"),
                        num_queries=point_config.get("num_queries", 1),
                        query_mix=point_config.get("query_mix"),
                        full_config=point_config,
                    )
                    configs.append(rc)

    return configs


def expand_experiment(experiment: ExperimentFile) -> list[RunConfig]:
    """Expand all variants in an experiment into RunConfigs."""
    all_configs: list[RunConfig] = []
    for variant in experiment.variants:
        all_configs.extend(expand_variant(experiment, variant))
    return all_configs
