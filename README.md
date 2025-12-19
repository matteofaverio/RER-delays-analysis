# IDF RER delays — dataset & analysis

This repository builds a station-level delay panel for RER (A–E) using the IDFM PRIM Estimated Timetable endpoint, and enriches it with static covariates and historical weather.

## Repo layout
- `src/idf_rer/`: reusable library code
- `scripts/`: entrypoints
- `data/sample/`: small sample files committed for inspection
- `data/static/`: instructions for GTFS inputs (not committed by default)

### 1) Setup
```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -r requirements.txt
pip install -e .
