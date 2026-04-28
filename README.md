# FileAnalyze

Interactive analytics dashboard for delimited text datasets (CSV, pipe, tab) with profiling, charting, quality insights, and a scalable Data Grid.

## What this app does

- Loads source data plus metadata sidecar (`<filename>_Meta`)
- Generates quick field-level stats (parallelized)
- Builds chart-based analysis with auto chart suggestion
- Shows quality scorecard and top findings
- Provides a powerful Data Grid with:
  - dimension/measure filters
  - column hide/unhide drawer
  - copy-to-clipboard
  - CSV export
  - large-data safety guard + server-side paging mode

## Project structure

- `fileanalyze/app.py` - Dash app entrypoint and callbacks
- `fileanalyze/layouts/` - page and tab layout builders
- `fileanalyze/services/` - ingest, profile, filters, quality, chart services
- `fileanalyze/models/` - typed schemas
- `fileanalyze/utils/` - concurrency and I/O helpers
- `fileanalyze/assets/` - CSS and Help tutorial page
- `fileanalyze/tests/` - pytest test suite

## Requirements

- Python 3.10+ (recommended)
- Dependencies in `requirements.txt`
- Metadata file available for your dataset (`*_Meta`)

## Setup

```powershell
cd "<Your Directory>"
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```powershell
python -m fileanalyze.app
```

Then open the local URL printed in terminal.

## Testing

```powershell
python -m pytest fileanalyze\tests -q
```

## Basic usage flow

1. Enter file path and delimiter
2. Click **Load + Profile**
3. Use Visual Analysis tab to generate charts
4. Use Data Grid tab to apply filters and inspect records
5. Export filtered CSV or copy grid view to clipboard

## Notes for large datasets

- App uses guardrails for high row/cell counts
- Switches to server-side infinite row model for massive filtered outputs
- Use CSV export for full output when sampled preview is shown

## Help tutorial

In the app header, click **Help** to open the built-in tutorial page.
