# -*- coding: utf-8 -*-
"""
Pipeline único para:
1) descargar olas desde Copernicus Marine
2) descargar olas desde Puertos del Estado
3) descargar viento 10 m desde Open-Meteo
4) fusionar todo por punto y tiempo
5) generar un único meteo_points.json

Objetivo principal:
- Mantener compatibilidad con la estructura actual de meteo_points.json
- Conservar hs/tp/di como oleaje de Copernicus
- Añadir hs_pde/tp_pde/di_pde como oleaje de Puertos del Estado
- Mantener horizonte largo de Copernicus, corto de PDE y rellenar con null cuando falten datos
- Permitir a la web usar PDE+viento en corto plazo y Copernicus en medio plazo
"""

from __future__ import annotations

import json
import math
import os
import re
import tempfile
import threading
import time
import gc

# Evita sobre-paralelismo interno de librerías nativas (útil en GitHub Actions)
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("HDF5_USE_FILE_LOCKING", "FALSE")
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import copernicusmarine
import numpy as np
import pandas as pd
import requests
import xarray as xr


# =========================
# CONFIGURACIÓN GENERAL
# =========================

POINTS_FILE = "lonp_latp.txt"
METEO_OUTPUT_JSON = "meteo_points.json"

# Intersección temporal para garantizar que las 3 fuentes compartan exactamente el mismo time
KEEP_ONLY_COMMON_TIMES = False

# Horizonte por fuente
PAST_DAYS = 1
COPERNICUS_FORECAST_DAYS = 5
OPEN_METEO_FORECAST_DAYS = 5
PDE_FORECAST_DAYS = 3
PDE_FORECAST_HOURS = PDE_FORECAST_DAYS * 24
PDE_TOTAL_HOURS = (PAST_DAYS * 24) + PDE_FORECAST_HOURS

# --- Copernicus olas ---
COPERNICUS_DATASET_ID = "cmems_mod_ibi_wav_anfc_0.027deg_PT1H-i"
COPERNICUS_USERNAME = os.environ["COPERNICUS_USERNAME"]
COPERNICUS_PASSWORD = os.environ["COPERNICUS_PASSWORD"]
COPERNICUS_SEARCH_OFFSETS = [0.0, 0.02, 0.04, 0.06, 0.08]
MIN_VALID_RATIO = 0.30

# --- Open-Meteo viento ---
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# --- Puertos del Estado ---
PDE_REGIONS = {
    "gib": {
        "label": "Estrecho",
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_regional_gib/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_regional_gib/HOURLY/",
    },
    "bal": {
        "label": "Baleares",
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_regional_bal/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_regional_bal/HOURLY/",
    },
    "can": {
        "label": "Canarias",
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_regional_can/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_regional_can/HOURLY/",
    },
    "aib": {
        "label": "Resto/AIB",
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_regional_aib/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_regional_aib/HOURLY/",
    },
}
PDE_REGION_PRIORITY = ["gib", "bal", "can", "aib"]

# --- HTTP general / robustez ---
DEFAULT_HTTP_HEADERS = {
    "User-Agent": "meteoport/1.0 (+https://github.com/)"
}
RETRYABLE_HTTP_CODES = {500, 502, 503, 504}

# --- Rendimiento / concurrencia ---
MAX_WORKERS_WIND = int(os.getenv("METEOPORT_MAX_WORKERS_WIND", "8"))
MAX_WORKERS_PDE = int(os.getenv("METEOPORT_MAX_WORKERS_PDE", "1"))
MAX_WORKERS_PORT = int(os.getenv("METEOPORT_MAX_WORKERS_PORT", "1"))
MAX_WORKERS_TOP_LEVEL = int(os.getenv("METEOPORT_MAX_WORKERS_TOP_LEVEL", "3"))
MAX_WORKERS_COPERNICUS = int(os.getenv("METEOPORT_MAX_WORKERS_COPERNICUS", "1"))

HTTP_TIMEOUT_CATALOG = int(os.getenv("METEOPORT_HTTP_TIMEOUT_CATALOG", "60"))
HTTP_TIMEOUT_FILE = int(os.getenv("METEOPORT_HTTP_TIMEOUT_FILE", "120"))
HTTP_TIMEOUT_WIND = int(os.getenv("METEOPORT_HTTP_TIMEOUT_WIND", "60"))

SESSION_POOL_CONNECTIONS = int(os.getenv("METEOPORT_SESSION_POOL_CONNECTIONS", "32"))
SESSION_POOL_MAXSIZE = int(os.getenv("METEOPORT_SESSION_POOL_MAXSIZE", "32"))

INVENTORY_CACHE_DIR = Path(os.getenv("METEOPORT_CACHE_DIR", ".meteoport_cache"))
USE_INVENTORY_CACHE = os.getenv("METEOPORT_USE_INVENTORY_CACHE", "1") == "1"
INVENTORY_CACHE_TTL_SECONDS = int(os.getenv("METEOPORT_INVENTORY_CACHE_TTL_SECONDS", str(12 * 3600)))



_session_local = threading.local()


def get_requests_session() -> requests.Session:
    session = getattr(_session_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=SESSION_POOL_CONNECTIONS,
            pool_maxsize=SESSION_POOL_MAXSIZE,
            max_retries=0,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(DEFAULT_HTTP_HEADERS)
        _session_local.session = session
    return session


def ensure_cache_dir():
    if USE_INVENTORY_CACHE:
        INVENTORY_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_json_cache(cache_name: str) -> Optional[Dict]:
    if not USE_INVENTORY_CACHE:
        return None
    ensure_cache_dir()
    path = INVENTORY_CACHE_DIR / cache_name
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age > INVENTORY_CACHE_TTL_SECONDS:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_json_cache(cache_name: str, payload: Dict):
    if not USE_INVENTORY_CACHE:
        return
    ensure_cache_dir()
    path = INVENTORY_CACHE_DIR / cache_name
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


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

def is_port_point(point: Dict) -> bool:
    return "puerto" in str(point.get("name", "")).lower()



def build_forecast_index(records):
    idx = {}
    for rec in records or []:
        t = normalize_time_to_utc_z(rec.get("time"))
        if t:
            idx[t] = rec
    return idx



def is_retryable_http_error(exc: Exception) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True

    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        code = getattr(resp, "status_code", None)
        return code in RETRYABLE_HTTP_CODES

    return False



def remove_file_safely(path):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def get_utc_midnight_now():
    return datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)


def get_window_bounds_for_source(future_days: int):
    today = get_utc_midnight_now()
    start_dt = today - timedelta(days=PAST_DAYS)
    end_dt = today + timedelta(hours=(future_days * 24) - 1)
    return start_dt, end_dt


def parse_fc_dataset_name(name: str):
    m = re.match(r"HW-(\d{10})-B(\d{10})-FC\.nc", name)
    if not m:
        return None
    valid_dt = pd.to_datetime(m.group(1), format="%Y%m%d%H", utc=True)
    base_dt = pd.to_datetime(m.group(2), format="%Y%m%d%H", utc=True)
    return {
        "name": name,
        "valid_dt": valid_dt,
        "base_dt": base_dt,
        "valid_str": valid_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "base_str": base_dt.strftime("%Y%m%d%H"),
    }


def select_fc_files_for_window(dataset_names: List[str], start_dt: datetime, end_dt: datetime) -> Tuple[List[str], List[str], Optional[str]]:
    selected_by_valid = {}

    for name in dataset_names:
        meta = parse_fc_dataset_name(name)
        if meta is None:
            continue

        valid_dt = meta["valid_dt"]
        if valid_dt < pd.Timestamp(start_dt, tz="UTC") or valid_dt > pd.Timestamp(end_dt, tz="UTC"):
            continue

        prev = selected_by_valid.get(meta["valid_str"])
        if prev is None or meta["base_dt"] > prev["base_dt"]:
            selected_by_valid[meta["valid_str"]] = meta

    ordered = sorted(selected_by_valid.values(), key=lambda x: x["valid_dt"])
    files = [m["name"] for m in ordered]
    runs_used = sorted({m["base_str"] for m in ordered})
    latest_run = runs_used[-1] if runs_used else None
    return files, runs_used, latest_run


# =========================
# 1) COPERNICUS
# =========================


def temp_nc_name(point_id, attempt_idx):
    return f"wave_point_{point_id:03d}_try_{attempt_idx:02d}.nc"



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



def fetch_copernicus_candidate(req_lon, req_lat, point_id, attempt_idx, start_datetime, end_datetime):
    outfile = temp_nc_name(point_id, attempt_idx)

    copernicusmarine.subset(
        dataset_id=COPERNICUS_DATASET_ID,
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

    with xr.open_dataset(outfile, cache=False) as ds:
        ds.load()
        real_lon = safe_float(ds["longitude"].values)
        real_lat = safe_float(ds["latitude"].values)

        ds = ds.squeeze()

        hs = np.asarray(ds["VHM0"].values).reshape(-1)
        tp = np.asarray(ds["VTPK"].values).reshape(-1)
        di = np.asarray(ds["VMDR"].values).reshape(-1)
        times = np.asarray(ds["time"].values).reshape(-1)

        forecast = []
        valid_count = 0

        for j in range(len(times)):
            hs_v = round_or_none(hs[j], 2)
            tp_v = round_or_none(tp[j], 2)
            di_v = round_or_none(di[j], 2)

            if hs_v is not None or tp_v is not None or di_v is not None:
                valid_count += 1

            forecast.append({
                "time": normalize_time_to_utc_z(times[j]),
                "hs": hs_v,
                "tp": tp_v,
                "di": di_v,
            })

    total_count = len(forecast)
    valid_ratio = valid_count / total_count if total_count else 0.0
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
        "valid_count": valid_count,
        "total_count": total_count,
        "valid_ratio": round(valid_ratio, 4),
        "distance_to_selected_grid_km": distance_km,
        "temp_nc_file": outfile,
    }



def pick_best_candidate(candidates):
    if not candidates:
        return None

    return sorted(
        candidates,
        key=lambda c: (
            c["valid_ratio"],
            c["valid_count"],
            -(c["distance_to_selected_grid_km"] or 0.0),
        ),
        reverse=True,
    )[0]



def cleanup_temp_files(files):
    for ncfile in files:
        try:
            if ncfile and os.path.exists(ncfile):
                os.remove(ncfile)
        except Exception as e:
            print(f"No se pudo borrar {ncfile}: {e}")




def _download_single_copernicus_point(point, search_plan, start_datetime, end_datetime):
    point_id = point["point_id"]
    name = point["name"]
    base_lon = point["lon"]
    base_lat = point["lat"]

    print(f"\n[COPERNICUS PUNTO {point_id}] {name} | lon={base_lon}, lat={base_lat}")

    candidates = []
    errors = []
    temp_files = []

    for attempt_idx, (dlon, dlat) in enumerate(search_plan, start=1):
        req_lon = base_lon + dlon
        req_lat = base_lat + dlat

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
                f"válidos={candidate['valid_count']}/{candidate['total_count']} "
                f"({candidate['valid_ratio']:.1%})"
            )

            if candidate["valid_ratio"] >= MIN_VALID_RATIO:
                print("  ✔ celda válida encontrada, se detiene la búsqueda")
                break

        except Exception as e:
            err = f"intento {attempt_idx:02d} req=({req_lon:.5f},{req_lat:.5f}) -> {e}"
            errors.append(err)
            print(f"  ERROR {err}")

    best = pick_best_candidate(candidates)

    if best is None:
        return {
            "result": {
                "point_id": point_id,
                "name": name,
                "requested_lon": base_lon,
                "requested_lat": base_lat,
                "lon": None,
                "lat": None,
                "forecast": [],
                "error": "No se pudo obtener ningún candidato de olas Copernicus",
                "search_errors": errors,
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
        "wave_search_info": {
            "adjusted_request_point": adjusted,
            "used_request_lon": chosen_req_lon,
            "used_request_lat": chosen_req_lat,
            "distance_to_selected_grid_km": best["distance_to_selected_grid_km"],
            "valid_count": best["valid_count"],
            "total_count": best["total_count"],
            "valid_ratio": best["valid_ratio"],
            "attempts_made": len(candidates) + len(errors),
            "min_valid_ratio_target": MIN_VALID_RATIO,
        },
    }

    if errors:
        point_data["wave_search_errors"] = errors

    return {"result": point_data, "temp_files": temp_files}


def download_copernicus_wave_data(points):
    start_datetime_dt, end_datetime_dt = get_window_bounds_for_source(COPERNICUS_FORECAST_DAYS)

    start_datetime = start_datetime_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_datetime = end_datetime_dt.strftime("%Y-%m-%dT%H:%M:%S")

    print("\n" + "=" * 60)
    print("DESCARGA DE OLAS COPERNICUS")
    print("=" * 60)
    print(f"start_datetime = {start_datetime}")
    print(f"end_datetime   = {end_datetime}")

    all_points_data = []
    all_temp_files = []
    search_plan = offsets_to_try()

    workers = max(1, min(MAX_WORKERS_COPERNICUS, len(points)))
    print(f"Workers Copernicus: {workers}")

    if workers == 1:
        for point in points:
            payload = _download_single_copernicus_point(point, search_plan, start_datetime, end_datetime)
            all_points_data.append(payload["result"])
            all_temp_files.extend(payload["temp_files"])
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(_download_single_copernicus_point, point, search_plan, start_datetime, end_datetime)
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
# 2) OPEN-METEO
# =========================


def fetch_wind_forecast(lat, lon, max_retries=3, timeout=HTTP_TIMEOUT_WIND):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "wind_speed_10m,wind_direction_10m",
        "wind_speed_unit": "ms",
        "forecast_days": OPEN_METEO_FORECAST_DAYS,
        "past_days": PAST_DAYS,
        "timezone": "UTC",
    }

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = get_requests_session().get(
                OPEN_METEO_URL,
                params=params,
                timeout=timeout,
                headers=DEFAULT_HTTP_HEADERS,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            last_error = e
            print(f"  [reintento {attempt}/{max_retries}] error viento lat={lat}, lon={lon}: {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt)

    raise last_error



def _download_single_wind_point(point, total_points):
    print(
        f"Descargando viento punto {point['point_id']}/{total_points}: "
        f"{point['name']} ({point['lon']}, {point['lat']})"
    )

    try:
        data = fetch_wind_forecast(point["lat"], point["lon"])
        hourly = data.get("hourly", {})

        times = hourly.get("time", [])
        speeds = hourly.get("wind_speed_10m", [])
        directions = hourly.get("wind_direction_10m", [])

        records = []
        for t, s, d in zip(times, speeds, directions):
            records.append({
                "time": normalize_time_to_utc_z(t),
                "wind_speed_10m_ms": round_or_none(s, 2),
                "wind_direction_10m_deg": round_or_none(d, 2),
            })

        print(f"OK leído viento punto {point['point_id']} ({point['name']}): {len(records)} registros")
        return {
            "point_id": point["point_id"],
            "name": point["name"],
            "lon": point["lon"],
            "lat": point["lat"],
            "forecast": records,
        }

    except requests.RequestException as e:
        print(f"[ERROR] No se pudo descargar viento para {point['name']}: {e}")
        return {
            "point_id": point["point_id"],
            "name": point["name"],
            "lon": point["lon"],
            "lat": point["lat"],
            "forecast": [],
            "error": str(e),
        }


def download_wind_data(points):
    print("\n" + "=" * 60)
    print("DESCARGA DE VIENTO OPEN-METEO")
    print("=" * 60)

    if not points:
        return []

    total_points = len(points)
    workers = max(1, min(MAX_WORKERS_WIND, total_points))
    print(f"Workers viento: {workers}")

    all_data = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_download_single_wind_point, point, total_points) for point in points]
        for fut in as_completed(futures):
            all_data.append(fut.result())

    all_data.sort(key=lambda x: x["point_id"])
    return all_data


# =========================
# 3) PUERTOS DEL ESTADO
# =========================


def fetch_catalog_xml(catalog_url: str, timeout=HTTP_TIMEOUT_CATALOG, max_retries=4) -> str:
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            r = get_requests_session().get(catalog_url, timeout=timeout)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            last_error = e
            print(f"  [reintento {attempt}/{max_retries}] error catálogo PDE: {e}")
            if attempt < max_retries and is_retryable_http_error(e):
                time.sleep(3 * attempt)
            else:
                break

    raise last_error



def parse_dataset_names(catalog_xml: str) -> List[str]:
    names = re.findall(r'name="(HW-\d{10}-B\d{10}-FC\.nc)"', catalog_xml)
    if not names:
        raise RuntimeError("No se encontraron datasets HW-...-FC.nc en el catálogo de Puertos del Estado.")
    return sorted(set(names))



def choose_latest_run(dataset_names: List[str]) -> Tuple[str, List[str]]:
    by_run: Dict[str, List[str]] = {}

    for name in dataset_names:
        m = re.match(r"HW-(\d{10})-B(\d{10})-FC\.nc", name)
        if not m:
            continue
        base_time = m.group(2)
        by_run.setdefault(base_time, []).append(name)

    if not by_run:
        raise RuntimeError("No se pudo agrupar ningún dataset de PDE por run.")

    latest_run = sorted(by_run.keys())[-1]
    files = sorted(by_run[latest_run])
    return latest_run, files



def open_local_nc_from_url(url: str, timeout=HTTP_TIMEOUT_FILE, max_retries=5, backoff=8):
    last_error = None

    for attempt in range(1, max_retries + 1):
        tmp_name = None

        try:
            with get_requests_session().get(url, stream=True, timeout=timeout) as r:
                if r.status_code in RETRYABLE_HTTP_CODES:
                    raise requests.HTTPError(
                        f"{r.status_code} Server Error for url: {url}",
                        response=r,
                    )

                r.raise_for_status()

                with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
                    tmp_name = tmp.name
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            tmp.write(chunk)

            try:
                with xr.open_dataset(tmp_name, engine="netcdf4", cache=False) as ds:
                    ds.load()
                    ds_mem = ds.copy(deep=True)
            except Exception:
                with xr.open_dataset(tmp_name, engine="h5netcdf", cache=False) as ds:
                    ds.load()
                    ds_mem = ds.copy(deep=True)

            remove_file_safely(tmp_name)
            gc.collect()
            return ds_mem, None

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last_error = e
            remove_file_safely(tmp_name)

            if isinstance(e, requests.HTTPError):
                resp = getattr(e, "response", None)
                code = getattr(resp, "status_code", None)
                if code not in RETRYABLE_HTTP_CODES:
                    raise

            if attempt < max_retries:
                wait_s = backoff * attempt
                print(f"    Aviso: fallo temporal ({e}). Reintentando en {wait_s}s...", flush=True)
                time.sleep(wait_s)
            else:
                break

        except Exception:
            remove_file_safely(tmp_name)
            raise

    raise RuntimeError(f"No se pudo descargar tras {max_retries} intentos: {url}") from last_error



def get_nearest_indices(ds: xr.Dataset, points: List[Dict]) -> List[Tuple[int, int]]:
    lats = ds["latitude"].values
    lons = ds["longitude"].values

    idxs = []
    for point in points:
        ilat = int(np.abs(lats - point["lat"]).argmin())
        ilon = int(np.abs(lons - point["lon"]).argmin())
        idxs.append((ilat, ilon))
    return idxs



def get_pde_region_inventory() -> Dict[str, Dict]:
    cache_key = f"pde_region_inventory_v2_past{PAST_DAYS}_future{PDE_FORECAST_DAYS}.json"
    cached = load_json_cache(cache_key)
    if cached:
        return cached

    inventory = {}

    for region_key in PDE_REGION_PRIORITY:
        cfg = PDE_REGIONS[region_key]
        catalog_xml = fetch_catalog_xml(cfg["catalog_xml"])
        dataset_names = parse_dataset_names(catalog_xml)
        start_dt, end_dt = get_window_bounds_for_source(PDE_FORECAST_DAYS)
        files, runs_used, latest_run = select_fc_files_for_window(dataset_names, start_dt, end_dt)

        if not files:
            raise RuntimeError(f"La región PDE {region_key} no tiene ficheros horarios disponibles en la ventana solicitada")

        sample_name = files[0]
        sample_url = cfg["fileserver_base"] + sample_name
        ds, tmp_name = open_local_nc_from_url(sample_url)

        try:
            lats = np.asarray(ds["latitude"].values, dtype=float)
            lons = np.asarray(ds["longitude"].values, dtype=float)
            inventory[region_key] = {
                "key": region_key,
                "label": cfg["label"],
                "catalog_xml": cfg["catalog_xml"],
                "fileserver_base": cfg["fileserver_base"],
                "latest_run": latest_run,
                "runs_used": runs_used,
                "files": files,
                "sample_file": sample_name,
                "lon_min": float(np.min(lons)),
                "lon_max": float(np.max(lons)),
                "lat_min": float(np.min(lats)),
                "lat_max": float(np.max(lats)),
            }
        finally:
            try:
                ds.close()
            except Exception:
                pass
            remove_file_safely(tmp_name)

    save_json_cache(cache_key, inventory)
    return inventory



def point_in_region(point: Dict, region_meta: Dict) -> bool:
    lon = point["lon"]
    lat = point["lat"]
    return (
        region_meta["lon_min"] <= lon <= region_meta["lon_max"]
        and region_meta["lat_min"] <= lat <= region_meta["lat_max"]
    )



def assign_points_to_pde_regions(points: List[Dict], inventory: Dict[str, Dict]) -> Dict[str, List[Dict]]:
    grouped = {k: [] for k in PDE_REGION_PRIORITY}

    for point in points:
        assigned_region = None
        for region_key in PDE_REGION_PRIORITY:
            if point_in_region(point, inventory[region_key]):
                assigned_region = region_key
                break

        if assigned_region is None:
            assigned_region = "aib"

        point["pde_region"] = assigned_region
        grouped[assigned_region].append(point)

    return grouped




def _process_single_pde_hour(nc_name: str, url: str, points: List[Dict], nearest_idxs, point_meta_ready: bool):
    ds = None
    tmp_name = None
    try:
        ds, tmp_name = open_local_nc_from_url(url)

        point_meta = {}
        if not point_meta_ready:
            lats = ds["latitude"].values
            lons = ds["longitude"].values
            for point, (ilat, ilon) in zip(points, nearest_idxs):
                grid_lon = float(lons[ilon])
                grid_lat = float(lats[ilat])
                point_meta[point["point_id"]] = {
                    "lon": grid_lon,
                    "lat": grid_lat,
                    "distance_to_selected_grid_km": round(
                        haversine_km(point["lon"], point["lat"], grid_lon, grid_lat), 3
                    ),
                }

        m = re.match(r"HW-(\d{10})-B(\d{10})-FC\.nc", nc_name)
        valid_time = pd.to_datetime(m.group(1), format="%Y%m%d%H", utc=True) if m else pd.NaT
        valid_time_str = valid_time.strftime("%Y-%m-%dT%H:%M:%SZ") if not pd.isna(valid_time) else None

        records_by_pid = {}
        for point, (ilat, ilon) in zip(points, nearest_idxs):
            rec = {"time": valid_time_str}

            if "VHM0" in ds.variables:
                rec["hs_pde"] = round_or_none(ds["VHM0"].isel(time=0, latitude=ilat, longitude=ilon).values, 2)
            else:
                rec["hs_pde"] = None

            if "VTPK" in ds.variables:
                rec["tp_pde"] = round_or_none(ds["VTPK"].isel(time=0, latitude=ilat, longitude=ilon).values, 2)
            else:
                rec["tp_pde"] = None

            if "VMDR" in ds.variables:
                rec["di_pde"] = round_or_none(ds["VMDR"].isel(time=0, latitude=ilat, longitude=ilon).values, 2)
            else:
                rec["di_pde"] = None

            records_by_pid[point["point_id"]] = rec

        return {
            "nc_name": nc_name,
            "point_meta": point_meta,
            "records_by_pid": records_by_pid,
        }
    finally:
        if ds is not None:
            try:
                ds.close()
            except Exception:
                pass
        remove_file_safely(tmp_name)
        gc.collect()


def download_pde_wave_data_for_region(points: List[Dict], region_meta: Dict):
    print(f"\n[PDE {region_meta['key'].upper()}] {region_meta['label']}")
    print(f"Run PDE más reciente usado: B{region_meta['latest_run']}")
    print(f"Runs PDE usados: {[f'B{r}' for r in region_meta.get('runs_used', [])]}")
    print(f"Número de ficheros horarios a procesar: {len(region_meta['files'])}")
    print(
        f"Cobertura malla: lon=[{region_meta['lon_min']:.3f}, {region_meta['lon_max']:.3f}] | "
        f"lat=[{region_meta['lat_min']:.3f}, {region_meta['lat_max']:.3f}]"
    )

    point_meta = {}
    point_forecasts = {
        p["point_id"]: {
            "point_id": p["point_id"],
            "name": p["name"],
            "requested_lon": p["lon"],
            "requested_lat": p["lat"],
            "forecast": [],
        }
        for p in points
    }

    failed_hours = []

    sample_url = region_meta["fileserver_base"] + region_meta["sample_file"]
    sample_ds, sample_tmp = open_local_nc_from_url(sample_url)
    try:
        nearest_idxs = get_nearest_indices(sample_ds, points)
        lats = sample_ds["latitude"].values
        lons = sample_ds["longitude"].values
        for point, (ilat, ilon) in zip(points, nearest_idxs):
            grid_lon = float(lons[ilon])
            grid_lat = float(lats[ilat])
            point_meta[point["point_id"]] = {
                "lon": grid_lon,
                "lat": grid_lat,
                "distance_to_selected_grid_km": round(
                    haversine_km(point["lon"], point["lat"], grid_lon, grid_lat), 3
                ),
            }
    finally:
        try:
            sample_ds.close()
        except Exception:
            pass
        remove_file_safely(sample_tmp)

    workers = max(1, min(MAX_WORKERS_PDE, len(region_meta["files"])))
    print(f"Workers PDE para {region_meta['key']}: {workers}")

    if workers == 1:
        for k, nc_name in enumerate(region_meta["files"], start=1):
            url = region_meta["fileserver_base"] + nc_name
            print(f"[{k}/{len(region_meta['files'])}] Descarga/proceso {nc_name}", flush=True)
            try:
                result = _process_single_pde_hour(nc_name, url, points, nearest_idxs, True)
                for pid, rec in result["records_by_pid"].items():
                    point_forecasts[pid]["forecast"].append(rec)
            except Exception as e:
                print(f"    ERROR en {nc_name}: {e}", flush=True)
                failed_hours.append({
                    "file": nc_name,
                    "url": url,
                    "error": str(e),
                })
    else:
        futures = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for k, nc_name in enumerate(region_meta["files"], start=1):
                url = region_meta["fileserver_base"] + nc_name
                print(f"[{k}/{len(region_meta['files'])}] Cola descarga/proceso {nc_name}", flush=True)
                fut = executor.submit(_process_single_pde_hour, nc_name, url, points, nearest_idxs, True)
                futures[fut] = (k, nc_name, url)

            for fut in as_completed(futures):
                _, nc_name, url = futures[fut]
                try:
                    result = fut.result()
                    for pid, rec in result["records_by_pid"].items():
                        point_forecasts[pid]["forecast"].append(rec)
                except Exception as e:
                    print(f"    ERROR en {nc_name}: {e}", flush=True)
                    failed_hours.append({
                        "file": nc_name,
                        "url": url,
                        "error": str(e),
                    })

    out = []
    for point in points:
        pid = point["point_id"]
        meta = point_meta.get(pid, {})
        forecast = sorted(point_forecasts[pid]["forecast"], key=lambda r: r.get("time") or "")

        valid_count = sum(
            1 for r in forecast
            if r.get("hs_pde") is not None or r.get("tp_pde") is not None or r.get("di_pde") is not None
        )
        total_count = len(forecast)

        point_out = {
            "point_id": pid,
            "name": point["name"],
            "requested_lon": point["lon"],
            "requested_lat": point["lat"],
            "lon": meta.get("lon"),
            "lat": meta.get("lat"),
            "forecast": forecast,
            "pde_search_info": {
                "region_key": region_meta["key"],
                "region_label": region_meta["label"],
                "region_sample_file": region_meta["sample_file"],
                "region_latest_run": region_meta["latest_run"],
                "region_runs_used": region_meta.get("runs_used", []),
                "region_lon_min": region_meta["lon_min"],
                "region_lon_max": region_meta["lon_max"],
                "region_lat_min": region_meta["lat_min"],
                "region_lat_max": region_meta["lat_max"],
                "distance_to_selected_grid_km": meta.get("distance_to_selected_grid_km"),
                "valid_count": valid_count,
                "total_count": total_count,
                "valid_ratio": round(valid_count / total_count, 4) if total_count else 0.0,
                "attempts_made": len(region_meta["files"]),
                "failed_hours_count": len(failed_hours),
            },
        }

        if failed_hours:
            point_out["pde_failed_hours"] = failed_hours

        out.append(point_out)

    if failed_hours:
        print(f"\n[AVISO] Horas PDE no descargadas en región {region_meta['key']}:", flush=True)
        for item in failed_hours:
            print(f" - {item['file']} -> {item['error']}", flush=True)
        print("Se continúa el pipeline y esas horas quedarán como faltantes en PDE.", flush=True)

    return out


def download_pde_wave_data(points):
    print("\n" + "=" * 60)
    print("DESCARGA DE OLAS PUERTOS DEL ESTADO")
    print("=" * 60)

    inventory = get_pde_region_inventory()
    grouped_points = assign_points_to_pde_regions(points, inventory)

    print("Regiones PDE detectadas:")
    for region_key in PDE_REGION_PRIORITY:
        meta = inventory[region_key]
        print(
            f" - {region_key}: {meta['label']} | "
            f"lon=[{meta['lon_min']:.3f}, {meta['lon_max']:.3f}] | "
            f"lat=[{meta['lat_min']:.3f}, {meta['lat_max']:.3f}] | "
            f"run=B{meta['latest_run']}"
        )

    out = []
    for region_key in PDE_REGION_PRIORITY:
        region_points = grouped_points.get(region_key, [])
        if not region_points:
            continue

        print(f"\nAsignados a {region_key}: {[p['name'] for p in region_points]}")
        out.extend(download_pde_wave_data_for_region(region_points, inventory[region_key]))

    out.sort(key=lambda x: x['point_id'])
    return out


# =========================
# 4) AGITACION
# =========================

PORT_MESHES = {
    "valencia": {
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_local_a05b/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_local_a05b/HOURLY/",
    },
    "barcelona": {
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_local_a02/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_local_a02/HOURLY/",
    },
    "malaga": {
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_local_a17/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_local_a17/HOURLY/",
    },
    "tenerife": {
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_local_a08a/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_local_a08a/HOURLY/",
    },
    "laspalmas": {
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_local_a15b/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_local_a15b/HOURLY/",
    },
    "algeciras": {
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_local_sfp_a11/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_local_sfp_a11/HOURLY/",
    }
}

PORT_MESH_PRIORITY = list(PORT_MESHES.keys())


def get_coord_arrays(ds: xr.Dataset) -> Tuple[np.ndarray, np.ndarray]:
    lon_candidates = ["lon", "longitude", "LON"]
    lat_candidates = ["lat", "latitude", "LAT"]

    lon_name = next((n for n in lon_candidates if n in ds.variables or n in ds.coords), None)
    lat_name = next((n for n in lat_candidates if n in ds.variables or n in ds.coords), None)

    if lon_name is None or lat_name is None:
        raise KeyError("No se encontraron variables/coordenadas lon/lat en el dataset")

    lons = np.asarray(ds[lon_name].values)
    lats = np.asarray(ds[lat_name].values)
    return lons, lats


def get_nearest_indices_port(ds: xr.Dataset, points: List[Dict]) -> List[Tuple[int, int]]:
    lons, lats = get_coord_arrays(ds)

    if lons.ndim == 1 and lats.ndim == 1:
        out = []
        for p in points:
            ilon = int(np.abs(lons - p["lon"]).argmin())
            ilat = int(np.abs(lats - p["lat"]).argmin())
            out.append((ilat, ilon))
        return out

    if lons.ndim == 2 and lats.ndim == 2:
        out = []
        for p in points:
            dist2 = (lons - p["lon"]) ** 2 + (lats - p["lat"]) ** 2
            flat_idx = int(np.nanargmin(dist2))
            ilat, ilon = np.unravel_index(flat_idx, dist2.shape)
            out.append((int(ilat), int(ilon)))
        return out

    raise ValueError(f"Dimensiones lon/lat no soportadas: lon={lons.ndim}, lat={lats.ndim}")


def read_grid_lon_lat(lons: np.ndarray, lats: np.ndarray, ilat: int, ilon: int) -> Tuple[float, float]:
    if lons.ndim == 1 and lats.ndim == 1:
        return float(lons[ilon]), float(lats[ilat])
    return float(lons[ilat, ilon]), float(lats[ilat, ilon])


def extract_vhm0_value_port(ds: xr.Dataset, ilat: int, ilon: int):
    arr = np.asarray(ds["VHM0"].values)
    arr = np.squeeze(arr)

    if arr.ndim != 2:
        raise ValueError(f"VHM0 no es 2D tras squeeze; shape={arr.shape}")

    if ilat < 0 or ilon < 0 or ilat >= arr.shape[0] or ilon >= arr.shape[1]:
        return None

    val = arr[ilat, ilon]

    if np.ma.is_masked(val):
        return None

    try:
        val = float(val)
    except Exception:
        return None

    if np.isnan(val):
        return None

    return val


def _retry_sleep_seconds(attempt: int, base: int = 15, cap: int = 120) -> int:
    return min(base * (2 ** max(0, attempt - 1)), cap)


def fetch_catalog_xml_resilient(url: str, max_retries: int = 4):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return fetch_catalog_xml(url)
        except Exception as e:
            last_err = e
            wait_s = _retry_sleep_seconds(attempt)
            print(f"  [reintento {attempt}/{max_retries}] error catálogo PDE: {e}", flush=True)
            if attempt < max_retries:
                time.sleep(wait_s)
    raise last_err


def open_local_nc_from_url_resilient(url: str, max_retries: int = 3):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return open_local_nc_from_url(url)
        except Exception as e:
            last_err = e
            wait_s = _retry_sleep_seconds(attempt, base=10, cap=60)
            print(f"    [reintento {attempt}/{max_retries}] error descargando NC: {url} -> {e}", flush=True)
            if attempt < max_retries:
                time.sleep(wait_s)
    raise last_err


def get_port_mesh_inventory() -> Dict[str, Dict]:
    cache_key = f"port_mesh_inventory_v3_past{PAST_DAYS}_future{PDE_FORECAST_DAYS}.json"
    cached = load_json_cache(cache_key)
    if cached:
        return cached

    inventory = {}

    for mesh_key in PORT_MESH_PRIORITY:
        cfg = PORT_MESHES[mesh_key]

        try:
            catalog_xml = fetch_catalog_xml_resilient(cfg["catalog_xml"], max_retries=4)
            dataset_names = parse_dataset_names(catalog_xml)
            start_dt, end_dt = get_window_bounds_for_source(PDE_FORECAST_DAYS)
            files, runs_used, latest_run = select_fc_files_for_window(dataset_names, start_dt, end_dt)

            if not files:
                raise RuntimeError(
                    f"La malla portuaria {mesh_key} no tiene ficheros horarios disponibles en la ventana solicitada"
                )

            sample_name = files[0]
            sample_url = cfg["fileserver_base"] + sample_name
            ds, tmp_name = open_local_nc_from_url_resilient(sample_url, max_retries=3)

            try:
                lons, lats = get_coord_arrays(ds)

                inventory[mesh_key] = {
                    "key": mesh_key,
                    "catalog_xml": cfg["catalog_xml"],
                    "fileserver_base": cfg["fileserver_base"],
                    "latest_run": latest_run,
                    "runs_used": runs_used,
                    "files": files,
                    "sample_file": sample_name,
                    "lon_min": float(np.nanmin(lons)),
                    "lon_max": float(np.nanmax(lons)),
                    "lat_min": float(np.nanmin(lats)),
                    "lat_max": float(np.nanmax(lats)),
                    "available": True,
                }
            finally:
                try:
                    ds.close()
                except Exception:
                    pass
                remove_file_safely(tmp_name)

        except Exception as e:
            print(f"[AVISO] Malla portuaria no disponible ahora mismo: {mesh_key} -> {e}", flush=True)
            inventory[mesh_key] = {
                "key": mesh_key,
                "catalog_xml": cfg["catalog_xml"],
                "fileserver_base": cfg["fileserver_base"],
                "latest_run": None,
                "runs_used": [],
                "files": [],
                "sample_file": None,
                "lon_min": None,
                "lon_max": None,
                "lat_min": None,
                "lat_max": None,
                "available": False,
                "error": str(e),
            }

    save_json_cache(cache_key, inventory)
    return inventory


def point_in_port_mesh(point: Dict, mesh_meta: Dict) -> bool:
    if not mesh_meta.get("available"):
        return False

    lon = point["lon"]
    lat = point["lat"]
    return (
        mesh_meta["lon_min"] <= lon <= mesh_meta["lon_max"]
        and mesh_meta["lat_min"] <= lat <= mesh_meta["lat_max"]
    )


def assign_points_to_port_meshes(points: List[Dict], inventory: Dict[str, Dict]) -> Dict[str, List[Dict]]:
    grouped = {k: [] for k in PORT_MESH_PRIORITY}

    for point in points:
        assigned_mesh = None

        for mesh_key in PORT_MESH_PRIORITY:
            if point_in_port_mesh(point, inventory[mesh_key]):
                assigned_mesh = mesh_key
                break

        if assigned_mesh is None:
            point["port_mesh"] = None
            continue

        point["port_mesh"] = assigned_mesh
        grouped[assigned_mesh].append(point)

    return grouped


def _process_single_port_hour(nc_name: str, url: str, points: List[Dict], nearest_idxs):
    ds = None
    tmp_name = None
    try:
        ds, tmp_name = open_local_nc_from_url_resilient(url, max_retries=3)
        m = re.match(r"HW-(\d{10})-B(\d{10})-FC\.nc", nc_name)
        valid_time = pd.to_datetime(m.group(1), format="%Y%m%d%H", utc=True) if m else pd.NaT
        valid_time_str = valid_time.strftime("%Y-%m-%dT%H:%M:%SZ") if not pd.isna(valid_time) else None

        records_by_pid = {}
        for point, (ilat, ilon) in zip(points, nearest_idxs):
            rec = {"time": valid_time_str}

            if "VHM0" in ds.variables:
                rec["hs_port"] = round_or_none(extract_vhm0_value_port(ds, ilat, ilon), 2)
            else:
                rec["hs_port"] = None

            records_by_pid[point["point_id"]] = rec

        return {
            "nc_name": nc_name,
            "records_by_pid": records_by_pid,
        }
    finally:
        if ds is not None:
            try:
                ds.close()
            except Exception:
                pass
        remove_file_safely(tmp_name)
        gc.collect()


def download_port_agitation_for_mesh(points: List[Dict], mesh_meta: Dict):
    if not mesh_meta.get("available"):
        print(f"\n[AVISO] Se omite malla {mesh_meta['key']} por indisponibilidad temporal.", flush=True)
        out = []
        for p in points:
            out.append({
                "point_id": p["point_id"],
                "name": p["name"],
                "requested_lon": p["lon"],
                "requested_lat": p["lat"],
                "lon": None,
                "lat": None,
                "forecast": [],
                "port_error": f"Malla portuaria temporalmente no disponible: {mesh_meta.get('error', 'sin detalle')}",
            })
        return out

    print(f"\n[AGITACIÓN {mesh_meta['key'].upper()}]")
    print(f"Run más reciente usado: B{mesh_meta['latest_run']}")
    print(f"Runs usados: {[f'B{r}' for r in mesh_meta.get('runs_used', [])]}")
    print(f"Número de ficheros horarios a procesar: {len(mesh_meta['files'])}")
    print(
        f"Cobertura malla: lon=[{mesh_meta['lon_min']:.3f}, {mesh_meta['lon_max']:.3f}] | "
        f"lat=[{mesh_meta['lat_min']:.3f}, {mesh_meta['lat_max']:.3f}]"
    )

    point_meta = {}
    point_forecasts = {
        p["point_id"]: {
            "point_id": p["point_id"],
            "name": p["name"],
            "requested_lon": p["lon"],
            "requested_lat": p["lat"],
            "forecast": [],
        }
        for p in points
    }

    failed_hours = []

    sample_url = mesh_meta["fileserver_base"] + mesh_meta["sample_file"]
    sample_ds, sample_tmp = open_local_nc_from_url_resilient(sample_url, max_retries=3)
    try:
        nearest_idxs = get_nearest_indices_port(sample_ds, points)
        lons, lats = get_coord_arrays(sample_ds)

        for point, (ilat, ilon) in zip(points, nearest_idxs):
            grid_lon, grid_lat = read_grid_lon_lat(lons, lats, ilat, ilon)
            point_meta[point["point_id"]] = {
                "lon": grid_lon,
                "lat": grid_lat,
                "distance_to_selected_grid_km": round(
                    haversine_km(point["lon"], point["lat"], grid_lon, grid_lat), 3
                ),
            }
    finally:
        try:
            sample_ds.close()
        except Exception:
            pass
        remove_file_safely(sample_tmp)

    workers = max(1, min(MAX_WORKERS_PORT, len(mesh_meta["files"])))
    print(f"Workers agitación para {mesh_meta['key']}: {workers}")

    if workers == 1:
        for k, nc_name in enumerate(mesh_meta["files"], start=1):
            url = mesh_meta["fileserver_base"] + nc_name
            print(f"[{k}/{len(mesh_meta['files'])}] Descarga/proceso {nc_name}", flush=True)
            try:
                result = _process_single_port_hour(nc_name, url, points, nearest_idxs)
                for pid, rec in result["records_by_pid"].items():
                    point_forecasts[pid]["forecast"].append(rec)
            except Exception as e:
                print(f"    ERROR en {nc_name}: {e}", flush=True)
                failed_hours.append({
                    "file": nc_name,
                    "url": url,
                    "error": str(e),
                })
    else:
        futures = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for k, nc_name in enumerate(mesh_meta["files"], start=1):
                url = mesh_meta["fileserver_base"] + nc_name
                print(f"[{k}/{len(mesh_meta['files'])}] Cola descarga/proceso {nc_name}", flush=True)
                fut = executor.submit(_process_single_port_hour, nc_name, url, points, nearest_idxs)
                futures[fut] = (k, nc_name, url)

            for fut in as_completed(futures):
                _, nc_name, url = futures[fut]
                try:
                    result = fut.result()
                    for pid, rec in result["records_by_pid"].items():
                        point_forecasts[pid]["forecast"].append(rec)
                except Exception as e:
                    print(f"    ERROR en {nc_name}: {e}", flush=True)
                    failed_hours.append({
                        "file": nc_name,
                        "url": url,
                        "error": str(e),
                    })

    out = []
    for point in points:
        pid = point["point_id"]
        meta = point_meta.get(pid, {})
        forecast = sorted(point_forecasts[pid]["forecast"], key=lambda r: r.get("time") or "")

        valid_count = sum(1 for r in forecast if r.get("hs_port") is not None)
        total_count = len(forecast)

        point_out = {
            "point_id": pid,
            "name": point["name"],
            "requested_lon": point["lon"],
            "requested_lat": point["lat"],
            "lon": meta.get("lon"),
            "lat": meta.get("lat"),
            "forecast": forecast,
            "port_search_info": {
                "mesh_key": mesh_meta["key"],
                "mesh_sample_file": mesh_meta["sample_file"],
                "mesh_latest_run": mesh_meta["latest_run"],
                "mesh_runs_used": mesh_meta.get("runs_used", []),
                "mesh_lon_min": mesh_meta["lon_min"],
                "mesh_lon_max": mesh_meta["lon_max"],
                "mesh_lat_min": mesh_meta["lat_min"],
                "mesh_lat_max": mesh_meta["lat_max"],
                "distance_to_selected_grid_km": meta.get("distance_to_selected_grid_km"),
                "valid_count": valid_count,
                "total_count": total_count,
                "valid_ratio": round(valid_count / total_count, 4) if total_count else 0.0,
                "attempts_made": len(mesh_meta["files"]),
                "failed_hours_count": len(failed_hours),
            },
        }

        if failed_hours:
            point_out["port_failed_hours"] = failed_hours

        out.append(point_out)

    if failed_hours:
        print(f"\n[AVISO] Horas de agitación no descargadas en malla {mesh_meta['key']}:", flush=True)
        for item in failed_hours:
            print(f" - {item['file']} -> {item['error']}", flush=True)
        print("Se continúa el pipeline y esas horas quedarán como faltantes en agitación.", flush=True)

    return out


def download_port_agitation(points):
    print("\n" + "=" * 60)
    print("DESCARGA DE AGITACIÓN EN PUERTO")
    print("=" * 60)

    inventory = get_port_mesh_inventory()
    grouped_points = assign_points_to_port_meshes(points, inventory)

    print("Mallas portuarias detectadas:")
    for mesh_key in PORT_MESH_PRIORITY:
        meta = inventory[mesh_key]
        if meta.get("available"):
            print(
                f" - {mesh_key}: "
                f"lon=[{meta['lon_min']:.3f}, {meta['lon_max']:.3f}] | "
                f"lat=[{meta['lat_min']:.3f}, {meta['lat_max']:.3f}] | "
                f"run=B{meta['latest_run']}"
            )
        else:
            print(f" - {mesh_key}: NO DISPONIBLE ({meta.get('error', 'sin detalle')})")

    out = []

    for mesh_key in PORT_MESH_PRIORITY:
        mesh_points = grouped_points.get(mesh_key, [])
        if not mesh_points:
            continue

        print(f"\nAsignados a {mesh_key}: {[p['name'] for p in mesh_points]}")
        out.extend(download_port_agitation_for_mesh(mesh_points, inventory[mesh_key]))

    assigned_point_ids = {p["point_id"] for p in out}
    for p in points:
        if p["point_id"] not in assigned_point_ids:
            out.append({
                "point_id": p["point_id"],
                "name": p["name"],
                "requested_lon": p["lon"],
                "requested_lat": p["lat"],
                "lon": None,
                "lat": None,
                "forecast": [],
                "port_error": "Punto fuera de las mallas portuarias configuradas o malla no disponible",
            })

    out.sort(key=lambda x: x["point_id"])
    return out


# =========================
# 5) FUSIÓN FINAL
# =========================
def merge_all_sources(copernicus_points, pde_points, wind_points, agitation_points):
    print("\n" + "=" * 60)
    print("FUSIÓN FINAL: COPERNICUS + PDE + VIENTO + AGITACIÓN")
    print("=" * 60)

    cop_by_name = {str(p.get("name", "")).strip(): p for p in copernicus_points}
    pde_by_name = {str(p.get("name", "")).strip(): p for p in pde_points}
    wind_by_name = {str(p.get("name", "")).strip(): p for p in wind_points}
    agit_by_name = {str(p.get("name", "")).strip(): p for p in agitation_points}

    merged_points = []
    all_names = sorted(
        set(cop_by_name.keys()) |
        set(pde_by_name.keys()) |
        set(wind_by_name.keys()) |
        set(agit_by_name.keys())
    )

    total_cop_records = 0
    total_pde_records = 0
    total_wind_records = 0
    total_agit_records = 0
    total_common_records = 0

    for idx, name in enumerate(all_names, start=1):
        cop = cop_by_name.get(name)
        pde = pde_by_name.get(name)
        wind = wind_by_name.get(name)
        agit = agit_by_name.get(name)

        cop_forecast = cop.get("forecast", []) if cop else []
        pde_forecast = pde.get("forecast", []) if pde else []
        wind_forecast = wind.get("forecast", []) if wind else []
        agit_forecast = agit.get("forecast", []) if agit else []

        cop_idx = build_forecast_index(cop_forecast)
        pde_idx = build_forecast_index(pde_forecast)
        wind_idx = build_forecast_index(wind_forecast)
        agit_idx = build_forecast_index(agit_forecast)

        cop_times = set(cop_idx.keys())
        pde_times = set(pde_idx.keys())
        wind_times = set(wind_idx.keys())
        agit_times = set(agit_idx.keys())

        total_cop_records += len(cop_times)
        total_pde_records += len(pde_times)
        total_wind_records += len(wind_times)
        total_agit_records += len(agit_times)

        if KEEP_ONLY_COMMON_TIMES:
            selected_times = sorted(cop_times & pde_times & wind_times & agit_times)
        else:
            selected_times = sorted(cop_times | pde_times | wind_times | agit_times)

        total_common_records += len(cop_times & pde_times & wind_times)

        merged_forecast = []
        for t in selected_times:
            c = cop_idx.get(t, {})
            p = pde_idx.get(t, {})
            w = wind_idx.get(t, {})
            a = agit_idx.get(t, {})

            merged_forecast.append({
                "time": t,
                "hs": c.get("hs"),
                "tp": c.get("tp"),
                "di": c.get("di"),
                "hs_pde": p.get("hs_pde"),
                "tp_pde": p.get("tp_pde"),
                "di_pde": p.get("di_pde"),
                "hs_port": a.get("hs_port"),
                "wind_speed_10m_ms": w.get("wind_speed_10m_ms"),
                "wind_direction_10m_deg": w.get("wind_direction_10m_deg"),
            })

        base = cop or pde or wind or agit or {}
        point_id = base.get("point_id", idx)
        requested_lon = base.get("requested_lon", base.get("lon"))
        requested_lat = base.get("requested_lat", base.get("lat"))
        lon = base.get("lon", requested_lon)
        lat = base.get("lat", requested_lat)

        merged_point = {
            "point_id": point_id,
            "name": name,
            "requested_lon": requested_lon,
            "requested_lat": requested_lat,
            "lon": lon,
            "lat": lat,
            "forecast": merged_forecast,
            "merge_info": {
                "copernicus_records": len(cop_times),
                "pde_records": len(pde_times),
                "wind_records": len(wind_times),
                "agitacion_records": len(agit_times),
                "common_records": len(cop_times & pde_times & wind_times),
                "selected_records": len(selected_times),
                "copernicus_only_records": len(cop_times - pde_times - wind_times - agit_times),
                "pde_only_records": len(pde_times - cop_times - wind_times - agit_times),
                "wind_only_records": len(wind_times - cop_times - pde_times - agit_times),
                "agitacion_only_records": len(agit_times - cop_times - pde_times - wind_times),
                "missing_copernicus_point": cop is None,
                "missing_pde_point": pde is None,
                "missing_wind_point": wind is None,
                "missing_agitacion_point": agit is None,
            },
        }

        if cop and "wave_search_info" in cop:
            merged_point["wave_search_info"] = cop["wave_search_info"]
        if cop and "wave_search_errors" in cop:
            merged_point["wave_search_errors"] = cop["wave_search_errors"]

        if pde and "pde_search_info" in pde:
            merged_point["pde_search_info"] = pde["pde_search_info"]
        if pde and "pde_failed_hours" in pde:
            merged_point["pde_failed_hours"] = pde["pde_failed_hours"]

        if agit and "port_search_info" in agit:
            merged_point["port_search_info"] = agit["port_search_info"]
        if agit and "port_failed_hours" in agit:
            merged_point["port_failed_hours"] = agit["port_failed_hours"]
        if agit and "port_error" in agit:
            merged_point["port_error"] = agit["port_error"]

        if cop and "error" in cop:
            merged_point["wave_error"] = cop["error"]
        if pde and "error" in pde:
            merged_point["pde_error"] = pde["error"]
        if wind and "error" in wind:
            merged_point["wind_error"] = wind["error"]

        merged_points.append(merged_point)

        print(
            f"Punto {point_id} ({name}): "
            f"cop={len(cop_times)}, pde={len(pde_times)}, viento={len(wind_times)}, "
            f"agit={len(agit_times)}, comunes={len(cop_times & pde_times & wind_times)}"
        )

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "past_days": PAST_DAYS,
        "copernicus_forecast_days": COPERNICUS_FORECAST_DAYS,
        "copernicus_forecast_hours": (PAST_DAYS + COPERNICUS_FORECAST_DAYS) * 24,
        "open_meteo_forecast_days": OPEN_METEO_FORECAST_DAYS,
        "open_meteo_forecast_hours": (PAST_DAYS + OPEN_METEO_FORECAST_DAYS) * 24,
        "pde_forecast_days": PDE_FORECAST_DAYS,
        "pde_forecast_hours": PDE_TOTAL_HOURS,
        "keep_only_common_times": KEEP_ONLY_COMMON_TIMES,
        "points_total": len(merged_points),
        "total_copernicus_records": total_cop_records,
        "total_pde_records": total_pde_records,
        "total_wind_records": total_wind_records,
        "total_agitacion_records": total_agit_records,
        "total_common_records": total_common_records,
    }

    output = {
        "summary": summary,
        "points": merged_points,
    }

    with open(METEO_OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nOK. Archivo final guardado en: {METEO_OUTPUT_JSON}")
    return output

# =========================
# MAIN
# =========================


def main():
    print("\n🌊🌬️ METEO PIPELINE FUSIONADO: COPERNICUS + PDE + VIENTO + AGITACIÓN\n")

    if not Path(POINTS_FILE).exists():
        raise FileNotFoundError(f"No existe el archivo de puntos: {POINTS_FILE}")

    points = read_points(POINTS_FILE)
    if not points:
        raise ValueError("No se encontraron puntos válidos en el archivo de entrada.")

    print(f"Se han leído {len(points)} puntos desde {POINTS_FILE}")

    offshore_points = [p for p in points if not is_port_point(p)]
    port_points = [p for p in points if is_port_point(p)]

    print(f"Puntos offshore: {len(offshore_points)}")
    print(f"Puntos puerto: {len(port_points)}")
    print(
        "Configuración workers: "
        f"top={MAX_WORKERS_TOP_LEVEL}, copernicus={MAX_WORKERS_COPERNICUS}, "
        f"pde={MAX_WORKERS_PDE}, viento={MAX_WORKERS_WIND}, puerto={MAX_WORKERS_PORT}"
    )

    copernicus_points = []
    pde_points = []
    wind_points = []
    agitation_points = []

    # Más estable en entornos CI: ejecutar secuencialmente las partes que usan netCDF/xarray.
    if offshore_points:
        copernicus_points = download_copernicus_wave_data(offshore_points)
        pde_points = download_pde_wave_data(offshore_points)
        wind_points = download_wind_data(offshore_points)

    if port_points:
        agitation_points = download_port_agitation(port_points)

    merge_all_sources(
        copernicus_points,
        pde_points,
        wind_points,
        agitation_points
    )

    print("\nProceso completado correctamente.")


if __name__ == "__main__":
    main()
