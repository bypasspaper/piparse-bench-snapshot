"""Publication-quality matplotlib chart templates."""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from ..models import AxisScale, ChartType, Visualization

# ── Style ──────────────────────────────────────────────────────────────────────

# Serif fonts for paper figures, matching typical LaTeX style
STYLE = {
    "font.family": "serif",
    "font.serif": ["Computer Modern Roman", "DejaVu Serif", "Times New Roman"],
    "font.size": 10,
    "axes.titlesize": 11,
    "axes.labelsize": 10,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 9,
    "figure.figsize": (5.5, 3.5),
    "figure.dpi": 150,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linewidth": 0.5,
    "axes.spines.top": False,
    "axes.spines.right": False,
}

MARKERS = ["o", "s", "^", "D", "v", "P", "X", "*"]
COLORS = ["#1f77b4", "#d62728", "#2ca02c", "#ff7f0e", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]

BASELINE_COLOR = "#d62728"  # red — always used for the baseline series
SECOND_COLOR = "#2ca02c"    # green — used when there is exactly one non-baseline series


def assign_series_colors(
    series_names: list[str], baseline: str | None,
) -> dict[str, str]:
    """Map each series name to a hex color.

    Baseline is always red. If there is exactly one non-baseline series
    and a baseline is present, that series is green. Otherwise non-baseline
    series cycle through the remaining palette.
    """
    mapping: dict[str, str] = {}
    has_baseline = baseline is not None and baseline in series_names
    others = [s for s in series_names if not (has_baseline and s == baseline)]

    if has_baseline:
        mapping[baseline] = BASELINE_COLOR

    if has_baseline and len(others) == 1:
        mapping[others[0]] = SECOND_COLOR
        return mapping

    palette = [c for c in COLORS if c != BASELINE_COLOR]
    for i, name in enumerate(others):
        mapping[name] = palette[i % len(palette)]
    return mapping


def _apply_style():
    plt.rcParams.update(STYLE)


# ── Data helpers ───────────────────────────────────────────────────────────────

def _sort_x_numeric(x_values: list[str]) -> list[str]:
    """Try to sort x values numerically, fall back to string sort."""
    try:
        return sorted(x_values, key=float)
    except ValueError:
        return sorted(x_values)


def _sort_series_keys(keys: list[str]) -> list[str]:
    """Sort series keys numerically if possible, otherwise alphabetically."""
    try:
        return sorted(keys, key=float)
    except ValueError:
        return sorted(keys)


def _group_by_series(
    data: list[dict], metric: str
) -> dict[str, list[tuple[str, float]]]:
    """Group data rows into {series_name: [(x_value, metric_value), ...]}.

    Series are sorted numerically when all keys are numeric, otherwise alphabetically.
    """
    groups: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for row in data:
        val = row.get(metric)
        if val is not None:
            groups[row["series"]].append((row["x_value"], val))
    return {k: groups[k] for k in _sort_series_keys(list(groups.keys()))}


def _write_data_csv(
    path: Path, data: list[dict], metric: str
) -> None:
    """Write a companion data CSV for the figure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["x", "series", metric])
        for row in data:
            val = row.get(metric)
            if val is not None:
                writer.writerow([row["x_value"], row["series"], val])


def compute_shared_y_limits(
    datasets: list[list[dict]], metric: str, padding: float = 0.05,
) -> tuple[float, float] | None:
    """Compute shared Y axis limits across multiple datasets.

    Returns (y_min, y_max) with padding, or None if no data.
    """
    all_vals = [
        row[metric] for data in datasets for row in data
        if row.get(metric) is not None
    ]
    if not all_vals:
        return None
    lo, hi = min(all_vals), max(all_vals)
    margin = (hi - lo) * padding if hi != lo else abs(hi) * padding or 1.0
    return (lo - margin, hi + margin)


# ── Chart implementations ─────────────────────────────────────────────────────

def line_chart(
    data: list[dict],
    metric: str,
    viz: Visualization,
    x_label: str,
    y_label: str,
    output_path: Path,
    series_label: str | None = None,
    y_limits: tuple[float, float] | None = None,
    baseline: str | None = None,
) -> Path:
    """Render a line chart with one line per series."""
    _apply_style()
    fig, ax = plt.subplots()

    series_data = _group_by_series(data, metric)
    color_map = assign_series_colors(list(series_data.keys()), baseline)

    for i, (series_name, points) in enumerate(series_data.items()):
        x_vals_sorted = _sort_x_numeric([p[0] for p in points])
        val_map = {p[0]: p[1] for p in points}
        xs = x_vals_sorted
        ys = [val_map[x] for x in xs]

        try:
            xs_num = [float(x) for x in xs]
        except ValueError:
            xs_num = list(range(len(xs)))

        ax.plot(
            xs_num, ys,
            marker=MARKERS[i % len(MARKERS)],
            color=color_map.get(series_name, COLORS[i % len(COLORS)]),
            label=series_name,
            linewidth=1.5,
            markersize=5,
        )

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

    if viz.x_scale == AxisScale.LOG:
        ax.set_xscale("log", base=2)
        ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
    if viz.y_scale == AxisScale.LOG:
        ax.set_yscale("log")

    if y_limits is not None:
        ax.set_ylim(y_limits)

    if len(series_data) > 1:
        ax.legend(loc=viz.legend_position or "best", title=series_label)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    return output_path


def bar_chart(
    data: list[dict],
    metric: str,
    viz: Visualization,
    x_label: str,
    y_label: str,
    output_path: Path,
    series_label: str | None = None,
    y_limits: tuple[float, float] | None = None,
    baseline: str | None = None,
) -> Path:
    """Render a bar chart (single series, one bar per x-value)."""
    _apply_style()
    fig, ax = plt.subplots()

    series_data = _group_by_series(data, metric)
    # For a single-series bar chart, take the first (or only) series
    if series_data:
        series_name, points = next(iter(series_data.items()))
    else:
        series_name, points = None, []

    x_labels = _sort_x_numeric([p[0] for p in points])
    val_map = {p[0]: p[1] for p in points}
    ys = [val_map[x] for x in x_labels]

    bar_color = BASELINE_COLOR if series_name is not None and series_name == baseline else COLORS[0]
    bars = ax.bar(range(len(x_labels)), ys, color=bar_color, width=0.6)
    ax.set_xticks(range(len(x_labels)))
    ax.set_xticklabels(x_labels, rotation=30 if max(len(l) for l in x_labels) > 8 else 0, ha="right" if max(len(l) for l in x_labels) > 8 else "center")
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

    # Add value labels on top of bars
    for bar, y in zip(bars, ys):
        ax.text(
            bar.get_x() + bar.get_width() / 2, y,
            f"{y:.0f}",
            ha="center", va="bottom", fontsize=8,
        )

    if viz.y_scale == AxisScale.LOG:
        ax.set_yscale("log")

    if y_limits is not None:
        ax.set_ylim(y_limits)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    return output_path


def grouped_bar(
    data: list[dict],
    metric: str,
    viz: Visualization,
    x_label: str,
    y_label: str,
    output_path: Path,
    series_label: str | None = None,
    y_limits: tuple[float, float] | None = None,
    baseline: str | None = None,
) -> Path:
    """Render a grouped bar chart with one bar group per x-value, one bar per series."""
    _apply_style()
    fig, ax = plt.subplots()

    series_data = _group_by_series(data, metric)
    series_names = list(series_data.keys())
    n_series = len(series_names)
    color_map = assign_series_colors(series_names, baseline)

    if not series_names:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path)
        plt.close(fig)
        return output_path

    # Collect all x values
    all_x = set()
    for points in series_data.values():
        all_x.update(p[0] for p in points)
    x_labels = _sort_x_numeric(list(all_x))

    bar_width = 0.8 / n_series
    x_positions = list(range(len(x_labels)))

    for i, series_name in enumerate(series_names):
        val_map = {p[0]: p[1] for p in series_data[series_name]}
        ys = [val_map.get(x, 0) for x in x_labels]
        offsets = [pos + i * bar_width - (n_series - 1) * bar_width / 2 for pos in x_positions]
        ax.bar(
            offsets, ys,
            width=bar_width,
            color=color_map.get(series_name, COLORS[i % len(COLORS)]),
            label=series_name,
        )

    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels, rotation=30 if x_labels and max(len(l) for l in x_labels) > 8 else 0, ha="right" if x_labels and max(len(l) for l in x_labels) > 8 else "center")
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

    if viz.y_scale == AxisScale.LOG:
        ax.set_yscale("log")

    if y_limits is not None:
        ax.set_ylim(y_limits)

    ax.legend(loc=viz.legend_position or "best", title=series_label)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    return output_path


def heatmap(
    data: list[dict],
    metric: str,
    viz: Visualization,
    x_label: str,
    y_label: str,
    output_path: Path,
    series_label: str | None = None,
    y_limits: tuple[float, float] | None = None,
    baseline: str | None = None,
) -> Path:
    """Render a heatmap for cartesian axis data (x_value = 'A / B')."""
    _apply_style()

    # Parse compound x_values: "ValA / ValB"
    points = [(row["x_value"], row.get(metric)) for row in data if row.get(metric) is not None]
    if not points:
        fig, ax = plt.subplots()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path)
        plt.close(fig)
        return output_path

    xs, ys_raw = set(), set()
    values: dict[tuple[str, str], float] = {}
    for label, val in points:
        parts = label.split(" / ", 1)
        if len(parts) == 2:
            a, b = parts
        else:
            a, b = label, ""
        xs.add(a)
        ys_raw.add(b)
        values[(a, b)] = val

    x_labels = sorted(xs)
    y_labels = sorted(ys_raw)

    import numpy as np
    grid = np.zeros((len(y_labels), len(x_labels)))
    for yi, yl in enumerate(y_labels):
        for xi, xl in enumerate(x_labels):
            grid[yi, xi] = values.get((xl, yl), 0)

    fig, ax = plt.subplots()
    im = ax.imshow(grid, cmap="YlOrRd", aspect="auto")

    ax.set_xticks(range(len(x_labels)))
    ax.set_xticklabels(x_labels, rotation=30, ha="right")
    ax.set_yticks(range(len(y_labels)))
    ax.set_yticklabels(y_labels)
    ax.set_xlabel(viz.x_label or x_label)
    ax.set_ylabel(viz.y_label or y_label)

    # Annotate cells
    val_label = viz.value_label or metric
    for yi in range(len(y_labels)):
        for xi in range(len(x_labels)):
            ax.text(xi, yi, f"{grid[yi, xi]:.0f}", ha="center", va="center", fontsize=8)

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label(val_label)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    return output_path


# ── Dispatch ───────────────────────────────────────────────────────────────────

CHART_RENDERERS = {
    ChartType.LINE_CHART: line_chart,
    ChartType.BAR_CHART: bar_chart,
    ChartType.GROUPED_BAR: grouped_bar,
    ChartType.HEATMAP: heatmap,
}


def render_chart(
    data: list[dict],
    metric: str,
    viz: Visualization,
    x_label: str,
    y_label: str,
    output_path: Path,
    data_csv_path: Path | None = None,
    series_label: str | None = None,
    y_limits: tuple[float, float] | None = None,
    baseline: str | None = None,
) -> Path:
    """Render a chart based on the visualization spec.

    Always writes a companion data CSV alongside the figure.
    """
    # Write data CSV
    csv_path = data_csv_path or output_path.with_name(output_path.stem + "_data.csv")
    _write_data_csv(csv_path, data, metric)

    renderer = CHART_RENDERERS.get(viz.type)
    if renderer is None:
        raise ValueError(f"Unknown chart type: {viz.type}")

    return renderer(
        data, metric, viz, x_label, y_label, output_path,
        series_label=series_label, y_limits=y_limits, baseline=baseline,
    )
