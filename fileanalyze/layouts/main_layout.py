"""Dash layout builders for File Analyze."""

from __future__ import annotations

from dash import dcc, html
import dash_bootstrap_components as dbc


def build_layout() -> html.Div:
    """
    Purpose:
        Build the full two-screen Dash layout for ingestion and analysis.

    Internal Logic:
        1. Renders ingestion controls at the top.
        2. Adds stores for session/run-scoped state.
        3. Renders two analysis tabs for visuals and grid exploration.

    Example invocation:
        layout = build_layout()
    """

    return html.Div(
        [
            dcc.Store(id="session-id"),
            dcc.Store(id="run-state"),
            dcc.Store(id="metadata-store"),
            dcc.Store(id="quick-stats-store"),
            dcc.Store(id="quality-store"),
            dcc.Store(id="schema-warnings-store"),
            html.Div(
                [
                    html.Div(
                        [
                            html.H2("File Analyze Dashboard", className="fa-title"),
                            html.P("Modern multi-user analytics studio for CSV/pipe datasets", className="fa-subtitle"),
                        ]
                    ),
                    html.A("Help", href="/assets/fileanalyze-help.html", target="_blank", className="fa-link"),
                ],
                className="fa-header",
            ),
            html.Div(
                [
                    html.Div(
                        [
                            dcc.Input(
                                id="input-file-path",
                                type="text",
                                placeholder="Enter source file path",
                                style={"width": "100%"},
                            ),
                            dcc.Dropdown(
                                id="input-delimiter",
                                options=[
                                    {"label": "Comma (,)", "value": ","},
                                    {"label": "Pipe (|)", "value": "|"},
                                    {"label": "Tab", "value": "\t"},
                                ],
                                value=",",
                            ),
                        ],
                        className="fa-grid-2",
                    ),
                    html.Button("Load and Profile", id="submit-load", n_clicks=0, className="fa-btn"),
                ],
                className="fa-card fa-stack",
                style={"marginBottom": "16px"},
            ),
            html.Div(id="load-status"),
            html.Div(id="sort-status", className="fa-muted", style={"marginBottom": "10px"}),
            dcc.Tabs(
                id="main-tabs",
                value="tab-visual",
                className="fa-tabs",
                children=[
                    dcc.Tab(label="Visualize", value="tab-visual", children=[_tab_visual()]),
                    dcc.Tab(label="Data Grid", value="tab-grid", children=[_tab_grid()]),
                ],
            ),
        ],
        className="fa-page",
    )


def _tab_visual() -> html.Div:
    """
    Purpose:
        Build Tab 1 visual analysis studio components.

    Internal Logic:
        1. Renders field lists grouped by dimensions and measures.
        2. Provides chart controls to choose fields and visual type.
        3. Reserves areas for figure, stats, and auto insights.

    Example invocation:
        tab = _tab_visual()
    """

    return html.Div(
        [
            html.Div(
                [
                    html.Div([html.H4("Dimensions"), html.Ul(id="dimension-field-list", className="fa-muted")], className="fa-card"),
                    html.Div([html.H4("Measures"), html.Ul(id="measure-field-list", className="fa-muted")], className="fa-card"),
                ],
                className="fa-grid-2",
            ),
            html.Div(
                [
                    dcc.Dropdown(id="chart-dimension", placeholder="Drop Dimension"),
                    dcc.Dropdown(id="chart-measure", placeholder="Drop Measure", style={"marginTop": "8px"}),
                    dcc.Dropdown(id="chart-color-dimension", placeholder="Optional second dimension", style={"marginTop": "8px"}),
                    dcc.Dropdown(
                        id="chart-type",
                        options=[
                            {"label": "Line", "value": "Line"},
                            {"label": "Pie", "value": "Pie"},
                            {"label": "Bar", "value": "Bar"},
                            {"label": "Stacked Bar", "value": "Stacked Bar"},
                            {"label": "Histogram", "value": "Histogram"},
                            {"label": "Scatter", "value": "Scatter"},
                        ],
                        value="Bar",
                        style={"marginTop": "8px"},
                    ),
                    html.Button("Generate Analysis", id="submit-chart", n_clicks=0, className="fa-btn"),
                ],
                className="fa-card fa-stack",
                style={"marginTop": "12px"},
            ),
            html.Div([dcc.Graph(id="analysis-chart", style={"marginTop": "12px"})], className="fa-card", style={"marginTop": "12px"}),
            html.Div(id="selected-stats"),
            html.Div(id="quality-summary", style={"marginTop": "10px"}),
        ]
    )


def _tab_grid() -> html.Div:
    """
    Purpose:
        Build Tab 2 with two-panel filter and data grid exploration.

    Internal Logic:
        1. Panel 1 provides live search inputs for dimensions and measures.
        2. Panel 2 renders filtered data table with row numbers.
        3. Includes export control for filtered subset output.

    Example invocation:
        tab = _tab_grid()
    """

    return html.Div(
        [
            dcc.Store(id="grid-dim-row-count", data=5),
            dcc.Store(id="grid-mea-row-count", data=5),
            html.Div(
                [
                    html.Div(
                        [
                            html.H4("Dimension filters", className="fa-filter-heading"),
                            html.Div(id="grid-dim-filters-container", className="fa-filter-group"),
                            html.Button(
                                "Add More Dimension Filters",
                                id="add-dim-filters",
                                n_clicks=0,
                                className="fa-btn fa-btn-ghost",
                                type="button",
                            ),
                        ],
                        className="fa-filter-col",
                    ),
                    html.Div(
                        [
                            html.H4("Measure filters", className="fa-filter-heading"),
                            html.Div(id="grid-mea-filters-container", className="fa-filter-group"),
                            html.Button(
                                "Add More Measure Filters",
                                id="add-mea-filters",
                                n_clicks=0,
                                className="fa-btn fa-btn-ghost",
                                type="button",
                            ),
                        ],
                        className="fa-filter-col",
                    ),
                ],
                className="fa-card fa-filter-split",
            ),
            html.Div(
                [
                    html.Button("Apply Filters", id="apply-filters", n_clicks=0, className="fa-btn"),
                    html.Button("Export Filtered CSV", id="export-filtered", n_clicks=0, className="fa-btn"),
                ],
                className="fa-card fa-filter-actions",
                style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "alignItems": "center", "marginTop": "8px"},
            ),
            dbc.Checklist(
                id="grid-sort-column-names",
                options=[{"label": "Sort the Column Names", "value": "on"}],
                value=[],
                inline=True,
                className="fa-checklist-sm",
            ),
            dcc.Store(id="grid-copy-content"),
            dcc.Store(id="grid-server-state"),
            dcc.Download(id="download-filtered-csv"),
            html.Div(id="copy-grid-feedback", style={"display": "none"}),
            html.Div(id="export-status", style={"marginTop": "8px"}),
            html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Button(
                                        "☰",
                                        id="grid-column-menu-toggle",
                                        n_clicks=0,
                                        className="fa-hamburger-btn",
                                        type="button",
                                        title="Show/Hide Columns",
                                    ),
                                    html.Button(
                                        "📋",
                                        id="copy-grid-button",
                                        n_clicks=0,
                                        className="fa-copy-icon-btn",
                                        type="button",
                                        title="Copy Entire Grid",
                                    ),
                                ],
                                className="fa-grid-left-controls",
                            ),
                            html.Div(id="data-grid-bottom-pane", className="fa-grid-table-content"),
                        ],
                        className="fa-grid-table-shell",
                    ),
                    dbc.Offcanvas(
                        [
                            html.H5("Hide / Unhide Columns", className="fa-filter-heading"),
                            dcc.Input(
                                id="grid-column-search",
                                type="text",
                                placeholder="Search columns...",
                                className="fa-column-search",
                            ),
                            html.Div(
                                [
                                    dbc.Checkbox(id="grid-col-select-all", value=False, className="fa-col-action-check"),
                                    html.Span("Select All", className="fa-col-action-label"),
                                    dbc.Checkbox(id="grid-col-deselect-all", value=False, className="fa-col-action-check"),
                                    html.Span("Deselect All", className="fa-col-action-label"),
                                ],
                                className="fa-col-actions",
                            ),
                            dbc.Checklist(
                                id="grid-visible-columns",
                                options=[],
                                value=[],
                                className="fa-col-checklist",
                            ),
                        ],
                        id="grid-column-offcanvas",
                        title="Columns",
                        is_open=False,
                        placement="start",
                        backdrop=True,
                        scrollable=True,
                    ),
                ],
                className="fa-card",
                style={"marginTop": "8px"},
            ),
        ]
    )

