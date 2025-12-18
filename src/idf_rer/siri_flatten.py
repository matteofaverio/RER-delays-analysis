from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
import pandas as pd


def _as_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _first(*vals: Any) -> Any:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def flatten_estimated_timetable(payload: Dict[str, Any]) -> pd.DataFrame:
    """
    Flatten a SIRI EstimatedTimetableDelivery payload into an event table.

    Output columns:
      snapshot_at_utc, stop_id, stop_name, line_code, direction, destination,
      scheduled_time_utc, rt_time_utc, delay_seconds, lead_time_seconds, vehicle_journey_id
    """
    siri = payload.get("Siri", payload)
    sd = siri.get("ServiceDelivery", {}) if isinstance(siri, dict) else {}

    snapshot_ts = _first(sd.get("ResponseTimestamp"), siri.get("ResponseTimestamp"))
    snapshot_at_utc = pd.to_datetime(snapshot_ts, utc=True, errors="coerce")

    deliveries = _as_list(sd.get("EstimatedTimetableDelivery"))
    rows: List[Dict[str, Any]] = []

    for d in deliveries:
        frames = _as_list(d.get("EstimatedJourneyVersionFrame"))
        for f in frames:
            journeys = _as_list(f.get("EstimatedVehicleJourney"))
            for j in journeys:
                line_code = _first(j.get("PublishedLineName"), j.get("LineRef"))
                destination = _first(j.get("DestinationName"), j.get("DestinationDisplay"))
                direction = _first(j.get("DirectionRef"))
                vjid = _first(j.get("VehicleJourneyRef noting"), j.get("VehicleJourneyRef"), j.get("DatedVehicleJourneyRef"))

                calls_container = j.get("EstimatedCalls")
                calls = []
                if isinstance(calls_container, dict):
                    calls = _as_list(calls_container.get("EstimatedCall"))
                else:
                    calls = _as_list(calls_container)

                for c in calls:
                    stop_id = _first(c.get("StopPointRef"), c.get("StopAreaRef"))
                    stop_name = _first(c.get("StopPointName"), c.get("StopAreaName"))

                    aimed = _first(c.get("AimedArrivalTime"), c.get("AimedDepartureTime"))
                    expected = _first(c.get("ExpectedArrivalTime"), c.get("ExpectedDepartureTime"))

                    sched = pd.to_datetime(aimed, utc=True, errors="coerce")
                    rt = pd.to_datetime(expected, utc=True, errors="coerce")

                    delay_s = None
                    lead_s = None
                    if pd.notna(sched) and pd.notna(rt):
                        delay_s = (rt - sched).total_seconds()
                    if pd.notna(snapshot_at_utc) and pd.notna(sched):
                        lead_s = (sched - snapshot_at_utc).total_seconds()

                    rows.append(
                        dict(
                            snapshot_at_utc=snapshot_at_utc,
                            stop_id=stop_id,
                            stop_name=stop_name,
                            line_code=line_code,
                            direction=direction,
                            destination=destination,
                            scheduled_time_utc=sched,
                            rt_time_utc=rt,
                            delay_seconds=delay_s,
                            lead_time_seconds=lead_s,
                            vehicle_journey_id=vjid,
                        )
                    )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Normalize dtypes
    df["snapshot_at_utc"] = pd.to_datetime(df["snapshot_at_utc"], utc=True, errors="coerce")
    df["scheduled_time_utc"] = pd.to_datetime(df["scheduled_time_utc"], utc=True, errors="coerce")
    df["rt_time_utc"] = pd.to_datetime(df["rt_time_utc"], utc=True, errors="coerce")
    df["delay_seconds"] = pd.to_numeric(df["delay_seconds"], errors="coerce")
    df["lead_time_seconds"] = pd.to_numeric(df["lead_time_seconds"], errors="coerce")

    return df
