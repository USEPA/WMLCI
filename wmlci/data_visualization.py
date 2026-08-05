"""
Plot WMLCI LCA result CSVs (summary, detail, characterized inventory).

"""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from wmlci.log import log
from wmlci.method_config import load_method_config
from wmlci.settings import find_versioned_file, resultspath

OTHER_LABEL = "Other"
TOTAL_LABEL = "Total"

_BLUE = "#0072B2"
_ORANGE = "#F5C16C"
_GREEN = "#009E73"
_PURPLE = "#CC79A7"
_YELLOW = "#E69F00"
_SKY = "#56B4E9"
_VERMILION = "#D55E00"
_MODEL_COLORS = [_BLUE, _ORANGE]
_FLOW_PALETTE = [_BLUE, _ORANGE, _GREEN, _PURPLE, _YELLOW, _SKY, _VERMILION]
_TOTAL_COLOR = _GREEN
_KNOWN_FLOW_COLORS = {
    "CO2": _BLUE,
    "CH4": _ORANGE,
    "N2O": _GREEN,
    "NOx": _SKY,
    "VOC": _YELLOW,
    "CO": _VERMILION,
    OTHER_LABEL: _PURPLE,
}

_PROCESS_SHORT = {
    "MSW landfilling of Food Waste; National average LFG recovery, "
    "typical collection, National average conditions": "Landfill (food waste)",
    "MSW combustion of Mixed Plastics": "Combustion (mixed plastics)",
    "MSW recycling of Mixed Plastics": "Recycling (mixed plastics)",
}


def short_process(name: str) -> str:
    """Short display label for a scenario process name."""
    return _PROCESS_SHORT.get(name, name)


def _method_meta(method_name: str) -> dict[str, Any]:
    """lcia_unit / lcia_label / flow groups from method YAML (with defaults)."""
    try:
        cfg = load_method_config(method_name)
    except FileNotFoundError:
        return {
            "lcia_unit": "",
            "lcia_label": method_name,
            "characterized_flow_groups": {},
        }
    return {
        "lcia_unit": str(cfg.get("lcia_unit") or ""),
        "lcia_label": str(cfg.get("lcia_label") or method_name),
        "characterized_flow_groups": dict(
            cfg.get("characterized_flow_groups") or {}
        ),
    }


def _score_unit(results: dict[str, Any]) -> str:
    unit = results.get("lcia_unit") or ""
    if not unit and results.get("summary") is not None:
        summary = results["summary"]
        if "score_unit" in summary.columns and len(summary):
            unit = str(summary["score_unit"].iloc[0] or "")
    if not unit and results.get("characterized") is not None:
        char = results["characterized"]
        if "characterized_unit" in char.columns and len(char):
            unit = str(char["characterized_unit"].iloc[0] or "")
    return unit or "impact"


def _impact_label(results: dict[str, Any]) -> str:
    return str(results.get("lcia_label") or results.get("method_name") or "Impact")


def load_results(method_name: str) -> dict[str, Any]:
    """Load summary, detail, and characterized-inventory CSVs for a method."""
    meta = _method_meta(method_name)
    try:
        out = (load_method_config(method_name).get("output_files") or {})
        summary_name = out.get(
            "summary_csv", f"{method_name}_lcia_results_summary.csv"
        )
        detail_name = out.get(
            "detail_csv", f"{method_name}_lcia_results_detailed.csv"
        )
        char_name = out.get(
            "characterized_inventory_csv",
            f"{method_name}_lcia_results_characterized_inventory.csv",
        )
    except FileNotFoundError:
        summary_name = f"{method_name}_lcia_results_summary.csv"
        detail_name = f"{method_name}_lcia_results_detailed.csv"
        char_name = f"{method_name}_lcia_results_characterized_inventory.csv"

    summary_path = find_versioned_file(resultspath, summary_name)
    detail_path = find_versioned_file(resultspath, detail_name)
    char_path = find_versioned_file(resultspath, char_name)
    if summary_path is None:
        raise FileNotFoundError(
            f"Summary results not found for '{method_name}' "
            f"(looked for {summary_name} / versioned variants under {resultspath})"
        )
    if detail_path is None:
        raise FileNotFoundError(
            f"Detail results not found for '{method_name}' "
            f"(looked for {detail_name} / versioned variants under {resultspath})"
        )

    log.info(f"Loading {method_name} summary: {summary_path.name}")
    log.info(f"Loading {method_name} detail:  {detail_path.name}")
    result = {
        "method_name": method_name,
        "summary": pd.read_csv(summary_path),
        "detail": pd.read_csv(detail_path),
        "summary_path": str(summary_path),
        "detail_path": str(detail_path),
        "characterized": None,
        "characterized_path": None,
        **meta,
    }
    if char_path is not None:
        log.info(f"Loading {method_name} characterized: {char_path.name}")
        result["characterized"] = pd.read_csv(char_path)
        result["characterized_path"] = str(char_path)
    else:
        log.info(f"No characterized inventory found for {method_name}")
    return result


def _as_results(method: str | dict[str, Any]) -> dict[str, Any]:
    return method if isinstance(method, dict) else load_results(method)


def _save(fig, savepath: str | Path | None):
    if savepath is not None:
        path = Path(savepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, bbox_inches="tight", format=path.suffix.lstrip(".") or None)
    return fig


def _annotate_bars(ax, *, horizontal: bool = False):
    """Label bar ends; skip near-zero patches (seaborn placeholders)."""
    for patch in ax.patches:
        if horizontal:
            val = patch.get_width()
            if pd.isna(val) or abs(val) < 1e-9:
                continue
            y = patch.get_y() + patch.get_height() / 2
            span = abs(ax.get_xlim()[1] - ax.get_xlim()[0])
            x = val + (0.015 * span if val >= 0 else -0.015 * span)
            ax.annotate(
                f"{val:.1f}",
                (x, y),
                ha="left" if val >= 0 else "right",
                va="center",
                fontsize=8,
                color="0.15",
            )
        else:
            val = patch.get_height()
            if pd.isna(val) or abs(val) < 1e-9:
                continue
            x = patch.get_x() + patch.get_width() / 2
            span = abs(ax.get_ylim()[1] - ax.get_ylim()[0])
            y = val + (0.02 * span if val >= 0 else -0.02 * span)
            ax.annotate(
                f"{val:.1f}",
                (x, y),
                ha="center",
                va="bottom" if val >= 0 else "top",
                fontsize=8,
                color="0.15",
            )


def _draw_scenario_category_dividers(ax, n_scenarios: int):
    """Solid vertical lines between categorical scenario groups on the x-axis."""
    if n_scenarios < 2:
        return
    for i in range(n_scenarios - 1):
        ax.axvline(
            i + 0.5,
            color="0.25",
            linewidth=1.25,
            linestyle="-",
            zorder=1,
        )


def _draw_panel_dividers(fig, axes):
    """Solid horizontal rules between stacked subplot panels."""
    axes = list(axes)
    if len(axes) < 2:
        return
    fig.canvas.draw()
    for ax in axes[:-1]:
        bbox = ax.get_position()
        fig.add_artist(
            plt.Line2D(
                [bbox.x0, bbox.x1],
                [bbox.y0, bbox.y0],
                transform=fig.transFigure,
                color="0.25",
                linewidth=1.5,
                solid_capstyle="butt",
            )
        )


def plot_scenario_scores(
    methods: str | list[str] | list[dict[str, Any]],
    *,
    savepath: str | Path | None = None,
):
    """Grouped bars of scenario impact totals; values labeled on each bar."""
    if isinstance(methods, str):
        methods = [methods]

    frames = []
    units = set()
    labels = []
    for m in methods:
        res = _as_results(m)
        df = res["summary"].copy()
        df["model"] = res["method_name"]
        df["scenario"] = df["process"].map(short_process)
        frames.append(df)
        units.add(_score_unit(res))
        labels.append(_impact_label(res))
    plot_df = pd.concat(frames, ignore_index=True)
    unit = units.pop() if len(units) == 1 else "impact"
    title = (
        f"Scenario {_impact_label(_as_results(methods[0]))}"
        if len(set(labels)) == 1
        else "Scenario impacts"
    )

    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(9, 5.5))
    sns.barplot(
        data=plot_df,
        x="scenario",
        y="score",
        hue="model",
        palette=_MODEL_COLORS[: plot_df["model"].nunique()],
        saturation=1,
        ax=ax,
    )
    ax.set_ylabel(f"Impact ({unit})")
    ax.set_xlabel("")
    ax.set_title(title)
    ax.axhline(0, color="0.4", linewidth=0.8)
    ax.tick_params(axis="x", labelrotation=15)
    if plot_df["model"].nunique() > 1:
        ax.legend(title="Model")
    else:
        legend = ax.get_legend()
        if legend is not None:
            legend.remove()
    ymin, ymax = ax.get_ylim()
    pad = (ymax - ymin) * 0.12
    ax.set_ylim(ymin - pad if ymin < 0 else ymin, ymax + pad)
    _draw_scenario_category_dividers(ax, plot_df["scenario"].nunique())
    _annotate_bars(ax)
    fig.tight_layout()
    return _save(fig, savepath)


def _top_contributor_panel_data(
    detail: pd.DataFrame,
    process: str,
    *,
    top_n: int,
    wrap: int = 55,
) -> pd.DataFrame:
    """Top ``top_n`` activities by |FlowAmount|, Other residual, and Total."""
    g = detail.loc[detail["process"] == process]
    if g.empty:
        return pd.DataFrame(columns=["activity", "activity_label", "FlowAmount"])
    total_amount = float(g["FlowAmount"].sum())
    g = g.sort_values("FlowAmount", key=lambda s: s.abs(), ascending=False)
    top = g.head(top_n)[["activity", "FlowAmount"]].sort_values(
        "FlowAmount", ascending=False
    )
    rest = g.iloc[top_n:]
    parts = [top]
    if len(rest):
        parts.append(
            pd.DataFrame(
                [
                    {
                        "activity": OTHER_LABEL,
                        "FlowAmount": rest["FlowAmount"].sum(),
                    }
                ]
            )
        )
    parts.append(
        pd.DataFrame([{"activity": TOTAL_LABEL, "FlowAmount": total_amount}])
    )
    out = pd.concat(parts, ignore_index=True)
    out["activity_label"] = out["activity"].map(
        lambda s: "\n".join(textwrap.wrap(str(s), width=wrap)) or str(s)
    )
    return out


def _draw_contributor_panel(
    ax,
    data: pd.DataFrame,
    *,
    title: str,
    color: str,
    unit: str,
    total_color: str = _TOTAL_COLOR,
):
    """Horizontal contributor bars; Total uses ``total_color``."""
    if data.empty:
        ax.set_title(title)
        ax.text(
            0.5,
            0.5,
            "No detail results",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )
        ax.set_xlabel(f"Contribution ({unit})")
        return
    palette = {
        row.activity_label: (
            total_color if row.activity == TOTAL_LABEL else color
        )
        for row in data.itertuples(index=False)
    }
    sns.barplot(
        data=data,
        y="activity_label",
        x="FlowAmount",
        hue="activity_label",
        palette=palette,
        saturation=1,
        dodge=False,
        legend=False,
        ax=ax,
        order=data["activity_label"].tolist(),
        orient="h",
    )
    ax.axvline(0, color="0.4", linewidth=0.8)
    ax.set_xlabel(f"Contribution ({unit})")
    ax.set_ylabel("")
    ax.set_title(title)
    ax.tick_params(axis="y", labelsize=8)
    xmin, xmax = ax.get_xlim()
    pad = (xmax - xmin) * 0.14 if xmax != xmin else 1.0
    ax.set_xlim(xmin - pad if xmin < 0 else xmin, xmax + pad)
    _annotate_bars(ax, horizontal=True)


def plot_top_contributors(
    method_a: str | dict[str, Any],
    method_b: str | dict[str, Any],
    process: str,
    *,
    top_n: int = 5,
    savepath: str | Path | None = None,
):
    """
    Two-panel horizontal bar chart: method_a on top, method_b below.

    Each panel shows that method's top ``top_n`` activities by |FlowAmount|
    (drawn high→low by signed value), an Other residual bar, and a Total bar.
    """
    a = _as_results(method_a)
    b = _as_results(method_b)

    panels = [
        (
            _top_contributor_panel_data(a["detail"], process, top_n=top_n),
            a["method_name"],
            _BLUE,
            _score_unit(a),
        ),
        (
            _top_contributor_panel_data(b["detail"], process, top_n=top_n),
            b["method_name"],
            _ORANGE,
            _score_unit(b),
        ),
    ]
    if all(data.empty for data, _, _, _ in panels):
        raise ValueError(f"No detail rows for process: {process}")

    sns.set_style("whitegrid")
    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=False)
    for ax, (data, name, color, unit) in zip(axes, panels):
        _draw_contributor_panel(ax, data, title=name, color=color, unit=unit)

    fig.suptitle(short_process(process), fontsize=13, fontweight="bold", y=0.98)
    fig.text(
        0.5,
        0.935,
        f'Top {top_n} contributors + aggregated "other" + total',
        ha="center",
        va="top",
        fontsize=10,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    _draw_panel_dividers(fig, axes)
    return _save(fig, savepath)


def plot_top_contributors_all_scenarios(
    method: str | dict[str, Any],
    *,
    top_n: int = 5,
    savepath: str | Path | None = None,
):
    """
    One figure with a panel per scenario: top ``top_n`` activities + Other + Total.

    Intended for single-method result sets (no model comparison).
    """
    res = _as_results(method)
    detail = res["detail"]
    processes = list(dict.fromkeys(res["summary"]["process"].tolist()))
    if not processes:
        raise ValueError(f"No scenarios in summary for '{res['method_name']}'")

    unit = _score_unit(res)
    impact = _impact_label(res)
    n = len(processes)
    sns.set_style("whitegrid")
    fig, axes = plt.subplots(n, 1, figsize=(14, 3.6 * n), sharex=False)
    if n == 1:
        axes = [axes]

    for ax, process in zip(axes, processes):
        data = _top_contributor_panel_data(
            detail, process, top_n=top_n, wrap=50
        )
        _draw_contributor_panel(
            ax,
            data,
            title=short_process(process),
            color=_BLUE,
            unit=unit,
        )

    fig.suptitle(
        f"{res['method_name']}: top contributors by scenario",
        fontsize=13,
        fontweight="bold",
        y=0.995,
    )
    fig.text(
        0.5,
        0.965,
        f'{impact} — top {top_n} activities + aggregated "other" + total',
        ha="center",
        va="top",
        fontsize=10,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    _draw_panel_dividers(fig, axes)
    return _save(fig, savepath)


def _assign_flow_group(flow_name: str, groups: dict[str, list[str]]) -> str:
    """Map elementary-flow name to a configured group label, else OTHER_LABEL."""
    name = str(flow_name)
    if name in groups:
        return name
    n = name.lower()
    for label, patterns in groups.items():
        for pat in patterns:
            p = str(pat).lower()
            if p and (p in n or n.strip() == p):
                return str(label)
    return OTHER_LABEL


def _flow_column(char: pd.DataFrame) -> str:
    """Prefer elementary_flow; accept legacy emission / emission-bucket CSVs."""
    for col in ("elementary_flow", "emission", "flow", "impact_flow"):
        if col in char.columns:
            return col
    raise KeyError(
        "Characterized inventory needs an elementary_flow (or legacy emission) column"
    )


def _prepare_flow_plot_frame(
    res: dict[str, Any],
    *,
    top_n: int,
) -> pd.DataFrame:
    """Scenario × flow_group characterized amounts for one method."""
    char = res.get("characterized")
    if char is None or len(char) == 0:
        raise FileNotFoundError(
            f"Characterized inventory CSV not found for '{res['method_name']}'. "
            "Re-run the LCA to write *_characterized_inventory.csv."
        )
    df = char.copy()
    flow_col = _flow_column(df)
    groups = res.get("characterized_flow_groups") or {}
    if groups:
        df["flow_group"] = df[flow_col].map(lambda n: _assign_flow_group(n, groups))
        order = list(groups.keys())
        if OTHER_LABEL not in order:
            order.append(OTHER_LABEL)
    else:
        totals = (
            df.groupby(flow_col, as_index=False)["characterized_amount"]
            .sum()
            .assign(_abs=lambda x: x["characterized_amount"].abs())
            .sort_values("_abs", ascending=False)
        )
        keep = set(totals.head(top_n)[flow_col].tolist())
        df["flow_group"] = df[flow_col].where(df[flow_col].isin(keep), OTHER_LABEL)
        order = [f for f in totals[flow_col].tolist() if f in keep]
        if OTHER_LABEL not in order and (df["flow_group"] == OTHER_LABEL).any():
            order.append(OTHER_LABEL)

    out = (
        df.assign(
            model=res["method_name"],
            scenario=df["process"].map(short_process),
        )
        .groupby(["model", "scenario", "flow_group"], as_index=False)[
            "characterized_amount"
        ]
        .sum()
    )
    out.attrs["flow_order"] = order
    return out


def _palette_for_flows(flow_names: list[str]) -> dict[str, str]:
    colors = {}
    i = 0
    for name in flow_names:
        if name in _KNOWN_FLOW_COLORS:
            colors[name] = _KNOWN_FLOW_COLORS[name]
        else:
            colors[name] = _FLOW_PALETTE[i % len(_FLOW_PALETTE)]
            i += 1
    return colors


def plot_scenario_by_flow(
    methods: str | list[str] | list[dict[str, Any]],
    *,
    top_n: int = 6,
    savepath: str | Path | None = None,
):
    """
    Scenario impact broken out by elementary flow (or configured flow groups).

    One panel per model. Uses ``characterized_flow_groups`` from the method YAML
    when set (e.g. CO2/CH4/N2O for GWP); otherwise top ``top_n`` flows + Other.
    Works for GHG, criteria air, water, land use, etc.
    """
    if isinstance(methods, str):
        methods = [methods]

    frames = []
    flow_orders: list[list[str]] = []
    units = set()
    labels = []
    for m in methods:
        res = _as_results(m)
        frame = _prepare_flow_plot_frame(res, top_n=top_n)
        frames.append(frame)
        flow_orders.append(list(frame.attrs.get("flow_order") or []))
        units.add(_score_unit(res))
        labels.append(_impact_label(res))

    plot_df = pd.concat(frames, ignore_index=True)
    flow_order: list[str] = []
    for order in flow_orders:
        for name in order:
            if name not in flow_order:
                flow_order.append(name)
    for name in plot_df["flow_group"].unique():
        if name not in flow_order:
            flow_order.append(str(name))

    unit = units.pop() if len(units) == 1 else "impact"
    impact = labels[0] if len(set(labels)) == 1 else "Impact"
    models = list(dict.fromkeys(plot_df["model"].tolist()))
    scenarios = list(dict.fromkeys(plot_df["scenario"].tolist()))
    palette = _palette_for_flows(flow_order)

    sns.set_style("whitegrid")
    n = len(models)
    fig, axes = plt.subplots(1, n, figsize=(5.5 * n, 5.5), sharey=True)
    if n == 1:
        axes = [axes]

    for ax, model in zip(axes, models):
        sub = plot_df.loc[plot_df["model"] == model]
        present = [f for f in flow_order if f in set(sub["flow_group"])]
        sns.barplot(
            data=sub,
            x="scenario",
            y="characterized_amount",
            hue="flow_group",
            hue_order=present,
            palette={k: palette[k] for k in present},
            saturation=1,
            ax=ax,
            order=scenarios,
        )
        ax.axhline(0, color="0.4", linewidth=0.8)
        ax.set_title(model)
        ax.set_xlabel("")
        # Make the impact unit explicit (e.g. kg CO2e), not only "characterized amount"
        ax.set_ylabel(f"{impact} ({unit})")
        ax.tick_params(axis="x", labelrotation=15)
        ymin, ymax = ax.get_ylim()
        pad = (ymax - ymin) * 0.12 if ymax != ymin else 1.0
        ax.set_ylim(ymin - pad if ymin < 0 else ymin, ymax + pad)
        _draw_scenario_category_dividers(ax, len(scenarios))
        _annotate_bars(ax)
        ax.legend(title="Flow", loc="best")

    fig.suptitle(f"Scenario {impact} by contributing flow", fontsize=12)
    fig.tight_layout()
    return _save(fig, savepath)


def plot_scenario_by_emission(
    methods: str | list[str] | list[dict[str, Any]],
    *,
    savepath: str | Path | None = None,
):
    """Alias for :func:`plot_scenario_by_flow` (legacy name)."""
    return plot_scenario_by_flow(methods, savepath=savepath)
