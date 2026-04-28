"""Chart factory for analysis visuals."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
from plotly.graph_objs import Figure


MODERN_COLORWAY: list[str] = [
    "#FF8A00",
    "#FFA94D",
    "#FFB347",
    "#FF9F1C",
    "#F77F00",
    "#FFB86B",
    "#F4A261",
    "#E76F51",
    "#FF7F50",
    "#FF9505",
]


def build_chart(
    dataframe: pd.DataFrame,
    chart_type: str,
    dimension: str,
    measure: str,
    color_dimension: str | None = None,
) -> Figure:
    """
    Purpose:
        Build a Plotly chart from selected fields and chart type.

    Internal Logic:
        1. Validates required dimension and measure inputs.
        2. Routes to type-specific Plotly constructors.
        3. Applies consistent theme and hover interactions.

    Example invocation:
        fig = build_chart(df, "Bar", "region", "sales")
    """

    if not dimension or not measure:
        return _empty_figure("Select both dimension and measure.")
    if dimension not in dataframe.columns or measure not in dataframe.columns:
        return _empty_figure("Selected fields are not available in the dataset.")

    work_df = dataframe[[dimension, measure] + ([color_dimension] if color_dimension else [])].copy()
    work_df[measure] = pd.to_numeric(work_df[measure], errors="coerce")
    work_df = work_df.dropna(subset=[measure])
    chart_key = chart_type.strip().lower()

    if chart_key == "line":
        fig = px.line(work_df, x=dimension, y=measure, color=color_dimension)
    elif chart_key == "pie":
        agg = work_df.groupby(dimension, dropna=False, as_index=False)[measure].sum()
        fig = px.pie(agg, names=dimension, values=measure)
    elif chart_key == "bar":
        agg = work_df.groupby(dimension, dropna=False, as_index=False)[measure].sum()
        fig = px.bar(agg, x=dimension, y=measure, color=(color_dimension or dimension))
    elif chart_key == "stacked bar":
        if not color_dimension:
            return _empty_figure("Stacked Bar requires a second dimension.")
        agg = work_df.groupby([dimension, color_dimension], dropna=False, as_index=False)[measure].sum()
        fig = px.bar(agg, x=dimension, y=measure, color=color_dimension)
    elif chart_key == "histogram":
        fig = px.histogram(work_df, x=measure, color=dimension)
    elif chart_key == "scatter":
        if not color_dimension:
            return _empty_figure("Scatter uses second dimension for color grouping.")
        fig = px.scatter(work_df, x=dimension, y=measure, color=color_dimension)
    else:
        return _empty_figure(f"Unsupported chart type: {chart_type}")

    fig.update_layout(
        template="plotly_white",
        margin={"l": 20, "r": 20, "t": 50, "b": 30},
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        colorway=MODERN_COLORWAY,
        font={"family": "Inter, Segoe UI, Arial, sans-serif", "size": 13, "color": "#000000"},
        hoverlabel={"bgcolor": "#e8f1ff", "font_size": 12, "font_color": "#000000"},
    )
    if chart_key in {"bar", "histogram"} and not color_dimension:
        # Keep multi-color bars by dimension while avoiding noisy legends.
        fig.update_layout(showlegend=False)
    fig.update_traces(marker={"line": {"color": "rgba(30, 58, 138, 0.15)", "width": 1}})
    fig.update_xaxes(separatethousands=True)
    fig.update_yaxes(separatethousands=True)
    return fig


def _empty_figure(message: str) -> Figure:
    """
    Purpose:
        Return a placeholder figure with a user-facing message.

    Internal Logic:
        1. Creates an empty scatter figure.
        2. Adds a centered annotation.
        3. Hides unnecessary axes for clean UI.

    Example invocation:
        fig = _empty_figure("No data")
    """

    fig = px.scatter()
    fig.add_annotation(text=message, showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.update_layout(
        template="plotly_white",
        margin={"l": 20, "r": 20, "t": 50, "b": 30},
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        font={"family": "Inter, Segoe UI, Arial, sans-serif", "size": 13, "color": "#000000"},
    )
    return fig

