# -*- coding: utf-8 -*-
"""
Pipeline exclusivo para descargar oleaje Copernicus GLOBAL (GLO)
y generar un único archivo oleaje_copernicus_glo.json.

Reglas:
- Usa solo el dataset global GLO.
- Ignora puntos cuyo nombre contenga "_puerto".
- Intenta primero el punto exacto.
- Si cae en tierra o no devuelve hs válida, busca alrededor solo un par de celdas.
- La variable que manda para aceptar o seguir buscando es hs_cop.
- Si no encuentra celda válida:
    hs_cop = null
    tp_cop = null
    di_cop = null
"""

from __future__ import annotations

import gc
import json
import math
import os

os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("HDF5_USE_FILE_LOCKING", "FALSE")

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import copernicusmarine
import numpy as np
import pandas as pd
import xarray as xr


# =========================
# CONFIGURACIÓN GENERAL
# =========================

POINTS_FILE = "lonp_latp.txt"
COP_OUTPUT_JSON = "2_olas_cop.json"

PAST_DAYS = 1
COP_FORECAST_DAYS = 5
COP_TOTAL_HOURS = (PAST_DAYS * 24) + (COP_FORECAST_DAYS * 24)

COPERNICUS_DATASET_ID_GLO = "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i"

# Recomendado: usar variables de entorno reales
# COPERNICUS_USERNAME = os.environ["COPERNICUS_USERNAME"]
# COPERNICUS_PASSWORD = os.environ["COPERNICUS_PASSWORD"]

COPERNICUS_USERNAME = os.getenv("COPERNICUS_USERNAME", "magulles2")
COPERNICUS_PASSWORD = os.getenv("COPERNICUS_PASSWORD", "MiguelCMEMS2017")

# GLO ~0.083º: exacto + alrededor
COPERNICUS_SEARCH_OFFSETS = [0.0, 0.083, 0.166, 0.332]

# El criterio de validez se basa SOLO en hs_cop
MIN_HS_VALID_RATIO = 0.50

# Se deja a 1 por robustez
MAX_WORKERS_COPERNICUS = 1


# =========================
# UTILIDADES GENERALES
# =========================

def safe_float(x):
    try:
        if isinstance(x, np.ndarray):
            x = np.asarray(x).reshape(-1)[0]
        x = float(x)
        if np.isnan(x):
            return None
        return x
    except Exception:
        return None


def round_or_none(value, digits=2):
    value = safe_float(value)
    if value is None:
        return None
    return round(value, digits)


def normalize_time_to_utc_z(value):
    if value is None:
        return None
    try:
        ts = pd.to_datetime(value, utc=True)
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def haversine_km(lon1, lat1, lon2, lat2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)

    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1 - a)))
    return r * c


def read_points(filename: str | Path) -> List[Dict]:
    points = []

    with open(filename, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split()
            if len(parts) != 3:
                print(f"[AVISO] Línea {line_num} ignorada: se esperaban 3 columnas y hay {len(parts)}")
                continue

            name, lon_str, lat_str = parts

            try:
                lon = float(lon_str)
                lat = float(lat_str)
            except ValueError:
                print(f"[AVISO] Línea {line_num} ignorada: lon/lat no válidos")
                continue

            points.append({
                "point_id": len(points) + 1,
                "name": str(name).strip(),
                "lon": lon,
                "lat": lat,
            })

    return points


def is_puerto_point(point: Dict) -> bool:
    return "_puerto" in str(point.get("name", "")).lower()


def get_utc_midnight_now():
    return datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)


def get_window_bounds_for_source(future_days: int):
    today = get_utc_midnight_now()
    start_dt = today - timedelta(days=PAST_DAYS)
    end_dt = today + timedelta(hours=(future_days * 24) - 1)
    return start_dt, end_dt


def temp_nc_name(point_id, attempt_idx):
    return f"cop_glo_point_{point_id:03d}_try_{attempt_idx:02d}.nc"


def cleanup_temp_files(files):
    for ncfile in files:
        try:
            if ncfile and os.path.exists(ncfile):
                os.remove(ncfile)
        except Exception as e:
            print(f"No se pudo borrar {ncfile}: {e}")


def offsets_to_try():
    combos = {(0.0, 0.0)}
    for d in COPERNICUS_SEARCH_OFFSETS:
        if d == 0:
            continue
        basic = [
            (d, 0.0), (-d, 0.0), (0.0, d), (0.0, -d),
            (d, d), (d, -d), (-d, d), (-d, -d),
        ]
        for item in basic:
            combos.add(item)

    return sorted(combos, key=lambda xy: (xy[0] ** 2 + xy[1] ** 2, abs(xy[0]), abs(xy[1])))


def open_copernicus_nc(path: str) -> xr.Dataset:
    try:
        with xr.open_dataset(path, engine="netcdf4", cache=False) as ds:
            ds.load()
            return ds.copy(deep=True)
    except Exception:
        with xr.open_dataset(path, engine="h5netcdf", cache=False) as ds:
            ds.load()
            return ds.copy(deep=True)


# =========================
# COPERNICUS GLO
# =========================

def fetch_copernicus_candidate(req_lon, req_lat, point_id, attempt_idx, start_datetime, end_datetime):
    outfile = temp_nc_name(point_id, attempt_idx)

    copernicusmarine.subset(
        dataset_id=COPERNICUS_DATASET_ID_GLO,
        username=COPERNICUS_USERNAME,
        password=COPERNICUS_PASSWORD,
        variables=["VHM0", "VTPK", "VMDR"],
        minimum_longitude=req_lon,
        maximum_longitude=req_lon,
        minimum_latitude=req_lat,
        maximum_latitude=req_lat,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        coordinates_selection_method="nearest",
        output_filename=outfile,
    )

    if not os.path.exists(outfile):
        raise RuntimeError(f"No se creó el archivo {outfile}")

    size_bytes = os.path.getsize(outfile)
    print(f"    archivo generado: {outfile} | {size_bytes} bytes")

    ds = open_copernicus_nc(outfile)
    try:
        real_lon = safe_float(ds["longitude"].values)
        real_lat = safe_float(ds["latitude"].values)

        ds = ds.squeeze()

        hs = np.asarray(ds["VHM0"].values).reshape(-1)
        tp = np.asarray(ds["VTPK"].values).reshape(-1)
        di = np.asarray(ds["VMDR"].values).reshape(-1)
        times = np.asarray(ds["time"].values).reshape(-1)

        forecast = []
        hs_valid_count = 0
        tp_valid_count = 0
        di_valid_count = 0

        for j in range(len(times)):
            hs_v = round_or_none(hs[j], 2)
            tp_v = round_or_none(tp[j], 2)
            di_v = round_or_none(di[j], 2)

            if hs_v is not None:
                hs_valid_count += 1
            if tp_v is not None:
                tp_valid_count += 1
            if di_v is not None:
                di_valid_count += 1

            forecast.append({
                "time": normalize_time_to_utc_z(times[j]),
                "hs_cop": hs_v,
                "tp_cop": tp_v,
                "di_cop": di_v,
            })

    finally:
        try:
            ds.close()
        except Exception:
            pass

    total_count = len(forecast)
    hs_valid_ratio = hs_valid_count / total_count if total_count else 0.0

    distance_km = None
    if None not in (req_lon, req_lat, real_lon, real_lat):
        distance_km = round(haversine_km(req_lon, req_lat, real_lon, real_lat), 3)

    gc.collect()

    return {
        "requested_lon": req_lon,
        "requested_lat": req_lat,
        "lon": real_lon,
        "lat": real_lat,
        "forecast": forecast,
        "hs_valid_count": hs_valid_count,
        "tp_valid_count": tp_valid_count,
        "di_valid_count": di_valid_count,
        "total_count": total_count,
        "hs_valid_ratio": round(hs_valid_ratio, 4),
        "distance_to_selected_grid_km": distance_km,
        "temp_nc_file": outfile,
    }


def pick_best_candidate(candidates):
    if not candidates:
        return None

    return sorted(
        candidates,
        key=lambda c: (
            c["hs_valid_ratio"],
            c["hs_valid_count"],
            -(c["distance_to_selected_grid_km"] or 0.0),
        ),
        reverse=True,
    )[0]


def _build_null_forecast(start_datetime_dt: datetime, end_datetime_dt: datetime) -> List[Dict]:
    out = []
    current = start_datetime_dt
    while current <= end_datetime_dt:
        out.append({
            "time": current.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "hs_cop": None,
            "tp_cop": None,
            "di_cop": None,
        })
        current += timedelta(hours=3)
    return out


def _download_single_copernicus_point(point, search_plan, start_datetime, end_datetime, start_datetime_dt, end_datetime_dt):
    point_id = point["point_id"]
    name = point["name"]
    base_lon = point["lon"]
    base_lat = point["lat"]

    print(f"\n[COPERNICUS GLO PUNTO {point_id}] {name} | lon={base_lon}, lat={base_lat}")

    candidates = []
    errors = []
    temp_files = []

    for attempt_idx, (dlon, dlat) in enumerate(search_plan, start=1):
        req_lon = base_lon + dlon
        req_lat = base_lat + dlat
        outfile = temp_nc_name(point_id, attempt_idx)

        try:
            candidate = fetch_copernicus_candidate(
                req_lon=req_lon,
                req_lat=req_lat,
                point_id=point_id,
                attempt_idx=attempt_idx,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )

            temp_files.append(candidate["temp_nc_file"])
            candidates.append(candidate)

            print(
                f"  intento {attempt_idx:02d}: req=({req_lon:.5f},{req_lat:.5f}) "
                f"-> grid=({candidate['lon']},{candidate['lat']}), "
                f"hs_valid={candidate['hs_valid_count']}/{candidate['total_count']} "
                f"({candidate['hs_valid_ratio']:.1%}), "
                f"tp_valid={candidate['tp_valid_count']}/{candidate['total_count']}, "
                f"di_valid={candidate['di_valid_count']}/{candidate['total_count']}"
            )

            if candidate["hs_valid_ratio"] >= MIN_HS_VALID_RATIO:
                print("  ✔ celda válida encontrada por hs, se detiene la búsqueda")
                break

        except Exception as e:
            size_info = ""
            if os.path.exists(outfile):
                try:
                    size_info = f" | tamaño={os.path.getsize(outfile)} bytes"
                except Exception:
                    pass

            err = (
                f"intento {attempt_idx:02d} "
                f"req=({req_lon:.5f},{req_lat:.5f}) "
                f"-> {type(e).__name__}: {repr(e)}{size_info}"
            )
            errors.append(err)
            print(f"  ERROR {err}")

    best = pick_best_candidate(candidates)

    if best is None or best["hs_valid_count"] == 0:
        null_forecast = _build_null_forecast(start_datetime_dt, end_datetime_dt)
        return {
            "result": {
                "point_id": point_id,
                "name": name,
                "requested_lon": base_lon,
                "requested_lat": base_lat,
                "lon": None,
                "lat": None,
                "forecast": null_forecast,
                "cop_search_info": {
                    "dataset_id": COPERNICUS_DATASET_ID_GLO,
                    "adjusted_request_point": False,
                    "used_request_lon": None,
                    "used_request_lat": None,
                    "distance_to_selected_grid_km": None,
                    "hs_valid_count": 0,
                    "tp_valid_count": 0,
                    "di_valid_count": 0,
                    "total_count": len(null_forecast),
                    "hs_valid_ratio": 0.0,
                    "attempts_made": len(candidates) + len(errors),
                    "min_hs_valid_ratio_target": MIN_HS_VALID_RATIO,
                },
                "cop_error": "No se pudo obtener ninguna celda válida de oleaje Copernicus GLO",
                "cop_search_errors": errors,
            },
            "temp_files": temp_files,
        }

    chosen_req_lon = best["requested_lon"]
    chosen_req_lat = best["requested_lat"]
    adjusted = not (abs(chosen_req_lon - base_lon) < 1e-12 and abs(chosen_req_lat - base_lat) < 1e-12)

    point_data = {
        "point_id": point_id,
        "name": name,
        "requested_lon": base_lon,
        "requested_lat": base_lat,
        "lon": best["lon"],
        "lat": best["lat"],
        "forecast": best["forecast"],
        "cop_search_info": {
            "dataset_id": COPERNICUS_DATASET_ID_GLO,
            "adjusted_request_point": adjusted,
            "used_request_lon": chosen_req_lon,
            "used_request_lat": chosen_req_lat,
            "distance_to_selected_grid_km": best["distance_to_selected_grid_km"],
            "hs_valid_count": best["hs_valid_count"],
            "tp_valid_count": best["tp_valid_count"],
            "di_valid_count": best["di_valid_count"],
            "total_count": best["total_count"],
            "hs_valid_ratio": best["hs_valid_ratio"],
            "attempts_made": len(candidates) + len(errors),
            "min_hs_valid_ratio_target": MIN_HS_VALID_RATIO,
        },
    }

    if errors:
        point_data["cop_search_errors"] = errors

    return {"result": point_data, "temp_files": temp_files}


def download_copernicus_glo_wave_data(points):
    start_datetime_dt, end_datetime_dt = get_window_bounds_for_source(COP_FORECAST_DAYS)

    start_datetime = start_datetime_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_datetime = end_datetime_dt.strftime("%Y-%m-%dT%H:%M:%S")

    print("\n" + "=" * 60)
    print("DESCARGA DE OLEAJE COPERNICUS GLO")
    print("=" * 60)
    print(f"dataset_id      = {COPERNICUS_DATASET_ID_GLO}")
    print(f"start_datetime  = {start_datetime}")
    print(f"end_datetime    = {end_datetime}")

    all_points_data = []
    all_temp_files = []
    search_plan = offsets_to_try()

    workers = max(1, min(MAX_WORKERS_COPERNICUS, len(points)))
    print(f"Workers Copernicus GLO: {workers}")

    if workers == 1:
        for point in points:
            payload = _download_single_copernicus_point(
                point, search_plan, start_datetime, end_datetime, start_datetime_dt, end_datetime_dt
            )
            all_points_data.append(payload["result"])
            all_temp_files.extend(payload["temp_files"])
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    _download_single_copernicus_point,
                    point,
                    search_plan,
                    start_datetime,
                    end_datetime,
                    start_datetime_dt,
                    end_datetime_dt,
                )
                for point in points
            ]
            for fut in as_completed(futures):
                payload = fut.result()
                all_points_data.append(payload["result"])
                all_temp_files.extend(payload["temp_files"])

    cleanup_temp_files(all_temp_files)
    all_points_data.sort(key=lambda x: x["point_id"])
    return all_points_data


# =========================
# GUARDADO FINAL
# =========================

def build_cop_output(cop_points: List[Dict]) -> Dict:
    total_cop_records = sum(len(p.get("forecast", [])) for p in cop_points)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "past_days": PAST_DAYS,
        "copernicus_forecast_days": COP_FORECAST_DAYS,
        "copernicus_forecast_hours": COP_TOTAL_HOURS,
        "copernicus_dataset_glo": COPERNICUS_DATASET_ID_GLO,
        "points_total": len(cop_points),
        "total_copernicus_records": total_cop_records,
    }

    return {
        "summary": summary,
        "points": cop_points,
    }


def save_output_json(output: Dict, output_file: str):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nOK. Archivo final guardado en: {output_file}")


# =========================
# MAIN
# =========================

def main():
    print("\n🌊 PIPELINE OLEAJE COPERNICUS GLO\n")

    if not Path(POINTS_FILE).exists():
        raise FileNotFoundError(f"No existe el archivo de puntos: {POINTS_FILE}")

    all_points = read_points(POINTS_FILE)
    if not all_points:
        raise ValueError("No se encontraron puntos válidos en el archivo de entrada.")

    points = [p for p in all_points if not is_puerto_point(p)]

    print(f"Se han leído {len(all_points)} puntos desde {POINTS_FILE}")
    print(f"Puntos filtrados sin '_puerto': {len(points)}")
    print(f"Configuración workers Copernicus: {MAX_WORKERS_COPERNICUS}")

    if not points:
        raise ValueError("No hay puntos válidos tras filtrar '_puerto'.")

    cop_points = download_copernicus_glo_wave_data(points)
    output = build_cop_output(cop_points)
    save_output_json(output, COP_OUTPUT_JSON)

    print("\nProceso completado correctamente.")


if __name__ == "__main__":
    main()