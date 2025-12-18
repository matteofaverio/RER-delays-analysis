from typing import Any, Dict, Iterable, List, Optional
from datetime import datetime, timezone
import re


def _parse_utc(ts: str) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    s = ts.strip()
    if not s:
        return None
    # handle "Z"
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _pick_time(call: Dict[str, Any], keys: Iterable[str]) -> Optional[str]:
    for k in keys:
        v = call.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _is_rer_line(line_code: str) -> bool:
    if not line_code:
        return False
    m = re.match(r"^\s*RER\s+([A-E])\s*$", line_code.strip(), flags=re.IGNORECASE)
    return m is not None


def flatten_estimated_timetable(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten PRIM 'estimated-timetable' SIRI payload into an event table.

    Output columns (per event):
      snapshot_at_utc, stop_id, stop_name, line_code, direction, destination,
      scheduled_time_utc, rt_time_utc, delay_seconds, lead_time_seconds, vehicle_journey_id
    """
    out: List[Dict[str, Any]] = []

    siri = payload.get("Siri", {}) if isinstance(payload, dict) else {}
    sd = siri.get("ServiceDelivery", {}) if isinstance(siri, dict) else {}

    snap_ts = sd.get("ResponseTimestamp")
    snap_dt = _parse_utc(snap_ts) if isinstance(snap_ts, str) else None
    if snap_dt is None:
        # If missing, keep deterministic but explicit
        snap_dt = datetime.now(timezone.utc)

    deliveries = sd.get("EstimatedTimetableDelivery", [])
    if isinstance(deliveries, dict):
        deliveries = [deliveries]
    if not isinstance(deliveries, list):
        return out

    for d in deliveries:
        frames = d.get("EstimatedJourneyVersionFrame", [])
        if isinstance(frames, dict):
            frames = [frames]
        if not isinstance(frames, list):
            continue

        for fr in frames:
            journeys = fr.get("EstimatedVehicleJourney", [])
            if isinstance(journeys, dict):
                journeys = [journeys]
            if not isinstance(journeys, list):
                continue

            for j in journeys:
                line_code = (
                    j.get("PublishedLineName")
                    or j.get("LineRef")
                    or ""
                )
                line_code = str(line_code).strip()

                if not _is_rer_line(line_code):
                    continue

                direction = str(j.get("DirectionName") or "").strip()
                destination = str(j.get("DestinationName") or j.get("DestinationRef") or "").strip()
                vjid = str(j.get("VehicleJourneyRef") or j.get("DatedVehicleJourneyRef") or "").strip()

                calls = j.get("EstimatedCall", [])
                if isinstance(calls, dict):
                    calls = [calls]
                if not isinstance(calls, list):
                    continue

                for c in calls:
                    stop_id = str(c.get("StopPointRef") or c.get("MonitoringRef") or "").strip()
                    stop_name = str(c.get("StopPointName") or "").strip()

                    aimed = _pick_time(c, ["AimedArrivalTime", "AimedDepartureTime"])
                    expected = _pick_time(c, ["ExpectedArrivalTime", "ExpectedDepartureTime"])

                    aimed_dt = _parse_utc(aimed) if aimed else None
                    exp_dt = _parse_utc(expected) if expected else None

                    delay_s: Optional[float] = None
                    lead_s: Optional[float] = None

                    if aimed_dt and exp_dt:
                        delay_s = (exp_dt - aimed_dt).total_seconds()
                    if aimed_dt:
                        lead_s = (aimed_dt - snap_dt).total_seconds()

                    out.append(
                        {
                            "snapshot_at_utc": snap_dt.isoformat().replace("+00:00", "Z"),
                            "stop_id": stop_id,
                            "stop_name": stop_name,
                            "line_code": line_code,
                            "direction": direction,
                            "destination": destination,
                            "scheduled_time_utc": aimed_dt.isoformat().replace("+00:00", "Z") if aimed_dt else "",
                            "rt_time_utc": exp_dt.isoformat().replace("+00:00", "Z") if exp_dt else "",
                            "delay_seconds": delay_s,
                            "lead_time_seconds": lead_s,
                            "vehicle_journey_id": vjid,
                        }
                    )

    return out
