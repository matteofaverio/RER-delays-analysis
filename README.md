# IDF RER delay panel (PRIM polling + GTFS enrichment)

This repository contains a reproducible pipeline to build a station/line delay panel for the Île-de-France RER network (A–E), starting from:
- the IDFM PRIM marketplace **Estimated Timetable** endpoint (SIRI EstimatedTimetableDelivery),
- the official IDFM GTFS bundle (stops + stop_extensions) to map quay identifiers to station-level metadata.

The code is designed to be:
- **scriptable** (CLI entrypoints under `scripts/`)
- **auditable** (explicit outputs, conservative filtering, no silent drops)
- **safe for public release** (API keys are read from `.env` and never committed)

## Quick start

### 1) Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements/base.txt
