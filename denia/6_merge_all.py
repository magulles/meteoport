# -*- coding: utf-8 -*-
"""
merge_all.py

Fusiona:
1_olas_pde.json
2_olas_cop.json
3_agitacion.json
4_olas_boyas.json
5_viento.json

Salida:
meteo_points_merged.json

Variables finales por tiempo y punto, en este orden:
[hs_pde; tp_pde; di_pde; hs_cop; tp_cop; di_cop; hs_puerto; hs_obs; wspeed_obs; wspeed_mod; wsdir_mod]
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any


# =========================
# CONFIG
# =========================

PDE_FILE = "1_olas_pde.json"
COP_FILE = "2_olas_cop.json"
PORT_FILE = "3_agitacion.json"
BUOYS_FILE = "4_olas_boyas.json"
WIND_FILE = "5_viento.json"

OUTPUT_FILE = "meteo_points_merged.json"


# =========================
# UTILIDADES
# =========================

def load_json(path: str | Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def norm_name(name: Any) -> str:
    return str(name).strip()


def safe_get(d: Dict[str, Any] | None, key: str, default=None):
    if not d:
        return default
    return d.get(key, default)


def round_or_none(value, digits=2):
    if value is None:
        return None
    try:
        v = float(value)
        return round(v, digits)
    except Exception:
        return None


def build_forecast_index(
    forecast: List[Dict[str, Any]],
    rename_map: Dict[str, str] | None = None
) -> Dict[str, Dict[str, Any]]:
    """
    Convierte una lista forecast a índice por time.
    rename_map permite renombrar claves de origen a claves destino.
    """
    out: Dict[str, Dict[str, Any]] = {}
    rename_map = rename_map or {}

    for row in forecast or []:
        t = row.get("time")
        if not t:
            continue

        rec = {}
        for k, v in row.items():
            if k == "time":
                continue
            new_k = rename_map.get(k, k)
            rec[new_k] = v

        out[t] = rec

    return out


def point_list_to_name_map(points: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {norm_name(p.get("name")): p for p in points or []}


def extract_buoy_points(buoys_json: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Convierte el formato:
    {
      "buoys": {
         "boya1": [{time, hsobs, wspd, lon, lat}, ...],
         ...
      }
    }

    a:
    {
      "boya1": {
         "name": "boya1",
         "lon": ...,
         "lat": ...,
         "forecast": [...]
      }
    }
    """
    out: Dict[str, Dict[str, Any]] = {}

    buoys = buoys_json.get("buoys", {})
    for buoy_name, records in buoys.items():
        name = norm_name(buoy_name)
        records = records or []

        lon = None
        lat = None
        if records:
            lon = records[0].get("lon")
            lat = records[0].get("lat")

        out[name] = {
            "name": name,
            "requested_lon": lon,
            "requested_lat": lat,
            "lon": lon,
            "lat": lat,
            "forecast": records,
        }

    return out


def choose_base_point(*sources: Dict[str, Any] | None) -> Dict[str, Any]:
    """
    Escoge el primer origen disponible para coordenadas/meta.
    Prioridad: PDE -> COP -> PORT -> WIND -> BUOY
    """
    for s in sources:
        if s:
            return s
    return {}


# =========================
# MERGE
# =========================

def merge_all(
    pde_json: Dict[str, Any],
    cop_json: Dict[str, Any],
    port_json: Dict[str, Any],
    buoys_json: Dict[str, Any],
    wind_json: Dict[str, Any],
) -> Dict[str, Any]:

    pde_by_name = point_list_to_name_map(pde_json.get("points", []))
    cop_by_name = point_list_to_name_map(cop_json.get("points", []))
    port_by_name = point_list_to_name_map(port_json.get("points", []))
    wind_by_name = point_list_to_name_map(wind_json.get("points", []))
    buoy_by_name = extract_buoy_points(buoys_json)

    all_names = sorted(
        set(pde_by_name.keys())
        | set(cop_by_name.keys())
        | set(port_by_name.keys())
        | set(wind_by_name.keys())
        | set(buoy_by_name.keys())
    )

    merged_points: List[Dict[str, Any]] = []

    total_records = 0

    for idx, name in enumerate(all_names, start=1):
        pde = pde_by_name.get(name)
        cop = cop_by_name.get(name)
        port = port_by_name.get(name)
        wind = wind_by_name.get(name)
        buoy = buoy_by_name.get(name)

        pde_idx = build_forecast_index(
            safe_get(pde, "forecast", []),
            rename_map={}
        )
        cop_idx = build_forecast_index(
            safe_get(cop, "forecast", []),
            rename_map={}
        )
        port_idx = build_forecast_index(
            safe_get(port, "forecast", []),
            rename_map={"hs_port": "hs_puerto"}
        )
        buoy_idx = build_forecast_index(
            safe_get(buoy, "forecast", []),
            rename_map={"hsobs": "hs_obs", "wspd": "wspeed_obs"}
        )
        wind_idx = build_forecast_index(
            safe_get(wind, "forecast", []),
            rename_map={
                "wind_speed_10m_ms": "wspeed_mod",
                "wind_direction_10m_deg": "wsdir_mod",
            }
        )

        all_times = sorted(
            set(pde_idx.keys())
            | set(cop_idx.keys())
            | set(port_idx.keys())
            | set(buoy_idx.keys())
            | set(wind_idx.keys())
        )

        base = choose_base_point(pde, cop, port, wind, buoy)

        merged_forecast: List[Dict[str, Any]] = []
        for t in all_times:
            p = pde_idx.get(t, {})
            c = cop_idx.get(t, {})
            a = port_idx.get(t, {})
            b = buoy_idx.get(t, {})
            w = wind_idx.get(t, {})

            merged_forecast.append({
                "time": t,
                "hs_pde": p.get("hs_pde"),
                "tp_pde": p.get("tp_pde"),
                "di_pde": p.get("di_pde"),
                "hs_cop": c.get("hs_cop"),
                "tp_cop": c.get("tp_cop"),
                "di_cop": c.get("di_cop"),
                "hs_puerto": a.get("hs_puerto"),
                "hs_obs": b.get("hs_obs"),
                "wspeed_obs": b.get("wspeed_obs"),
                "wspeed_mod": w.get("wspeed_mod"),
                "wsdir_mod": w.get("wsdir_mod"),
            })

        total_records += len(merged_forecast)

        merged_point = {
            "point_id": safe_get(base, "point_id", idx),
            "name": name,
            "requested_lon": safe_get(base, "requested_lon", safe_get(base, "lon")),
            "requested_lat": safe_get(base, "requested_lat", safe_get(base, "lat")),
            "lon": safe_get(base, "lon"),
            "lat": safe_get(base, "lat"),
            "forecast": merged_forecast,
            "merge_info": {
                "pde_records": len(pde_idx),
                "cop_records": len(cop_idx),
                "agitacion_records": len(port_idx),
                "boya_records": len(buoy_idx),
                "viento_records": len(wind_idx),
                "selected_records": len(all_times),
                "has_pde_point": pde is not None,
                "has_cop_point": cop is not None,
                "has_agitacion_point": port is not None,
                "has_boya_point": buoy is not None,
                "has_viento_point": wind is not None,
            }
        }

        # Conserva info útil de origen si existe
        if pde and "pde_search_info" in pde:
            merged_point["pde_search_info"] = pde["pde_search_info"]
        if cop and "cop_search_info" in cop:
            merged_point["cop_search_info"] = cop["cop_search_info"]
        if port and "port_search_info" in port:
            merged_point["port_search_info"] = port["port_search_info"]
        if wind and "wind_source" in wind:
            merged_point["wind_source"] = wind["wind_source"]

        merged_points.append(merged_point)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "points_total": len(merged_points),
        "total_records": total_records,
        "sources": {
            "pde_file": PDE_FILE,
            "cop_file": COP_FILE,
            "agitacion_file": PORT_FILE,
            "boyas_file": BUOYS_FILE,
            "viento_file": WIND_FILE,
        },
        "variables_order": [
            "hs_pde",
            "tp_pde",
            "di_pde",
            "hs_cop",
            "tp_cop",
            "di_cop",
            "hs_puerto",
            "hs_obs",
            "wspeed_obs",
            "wspeed_mod",
            "wsdir_mod",
        ],
        "merge_policy": {
            "point_match": "exact_name",
            "time_match": "exact_iso_timestamp_union",
            "missing_values": "null",
        }
    }

    return {
        "summary": summary,
        "points": merged_points,
    }


# =========================
# MAIN
# =========================

def main():
    for f in [PDE_FILE, COP_FILE, PORT_FILE, BUOYS_FILE, WIND_FILE]:
        if not Path(f).exists():
            raise FileNotFoundError(f"No existe el archivo requerido: {f}")

    pde_json = load_json(PDE_FILE)
    cop_json = load_json(COP_FILE)
    port_json = load_json(PORT_FILE)
    buoys_json = load_json(BUOYS_FILE)
    wind_json = load_json(WIND_FILE)

    merged = merge_all(
        pde_json=pde_json,
        cop_json=cop_json,
        port_json=port_json,
        buoys_json=buoys_json,
        wind_json=wind_json,
    )

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"OK. Archivo final guardado en: {OUTPUT_FILE}")
    print(f"Puntos fusionados: {merged['summary']['points_total']}")
    print(f"Registros totales: {merged['summary']['total_records']}")


if __name__ == "__main__":
    main()