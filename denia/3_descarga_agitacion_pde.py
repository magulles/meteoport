# -*- coding: utf-8 -*-
"""
Pipeline exclusivo para descargar agitación portuaria
y generar un único archivo agitacion_portuaria.json.

Solo procesa puntos cuyo nombre contenga la palabra clave "_puerto".

Salida:
- Estructura tipo meteo_points.json
- Un único bloque por punto con forecast horario de agitación:
    hs_port
"""

from __future__ import annotations

import gc
import json
import math
import os
import re
import tempfile
import threading
import time

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

import numpy as np
import pandas as pd
import requests
import xarray as xr


# =========================
# CONFIGURACIÓN GENERAL
# =========================

POINTS_FILE = "lonp_latp.txt"
PORT_OUTPUT_JSON = "3_agitacion.json"

PAST_DAYS = 1
PORT_FORECAST_DAYS = 3
PORT_FORECAST_HOURS = PORT_FORECAST_DAYS * 24
PORT_TOTAL_HOURS = (PAST_DAYS * 24) + PORT_FORECAST_HOURS

DEFAULT_HTTP_HEADERS = {
    "User-Agent": "meteoport/1.0 (+https://github.com/)"
}
RETRYABLE_HTTP_CODES = {500, 502, 503, 504}

MAX_WORKERS_PORT = int(os.getenv("METEOPORT_MAX_WORKERS_PORT", "1"))
HTTP_TIMEOUT_CATALOG = int(os.getenv("METEOPORT_HTTP_TIMEOUT_CATALOG", "60"))
HTTP_TIMEOUT_FILE = int(os.getenv("METEOPORT_HTTP_TIMEOUT_FILE", "120"))

SESSION_POOL_CONNECTIONS = int(os.getenv("METEOPORT_SESSION_POOL_CONNECTIONS", "32"))
SESSION_POOL_MAXSIZE = int(os.getenv("METEOPORT_SESSION_POOL_MAXSIZE", "32"))

INVENTORY_CACHE_DIR = Path(os.getenv("METEOPORT_CACHE_DIR", ".meteoport_cache"))
USE_INVENTORY_CACHE = os.getenv("METEOPORT_USE_INVENTORY_CACHE", "1") == "1"
INVENTORY_CACHE_TTL_SECONDS = int(os.getenv("METEOPORT_INVENTORY_CACHE_TTL_SECONDS", str(12 * 3600)))

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
    "algeciras": {
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_local_sfp_a11/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_local_sfp_a11/HOURLY/",
    },
    "laspalmas": {
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_local_a15b/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_local_a15b/HOURLY/",
    }
}

PORT_MESH_PRIORITY = list(PORT_MESHES.keys())

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


def is_puerto_point(point: Dict) -> bool:
    return "_puerto" in str(point.get("name", "")).lower()


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
    m = re.match(
        r'^(?P<prefix>[A-Za-z0-9_:-]+)-(?P<valid>\d{10})-B(?P<base>\d{10})-FC\.nc$',
        name
    )
    if not m:
        return None

    valid_dt = pd.to_datetime(m.group("valid"), format="%Y%m%d%H", utc=True)
    base_dt = pd.to_datetime(m.group("base"), format="%Y%m%d%H", utc=True)

    return {
        "name": name,
        "prefix": m.group("prefix"),
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
# AGITACIÓN PORTUARIA
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
            print(f"  [reintento {attempt}/{max_retries}] error catálogo puerto: {e}")
            if attempt < max_retries and is_retryable_http_error(e):
                time.sleep(3 * attempt)
            else:
                break

    raise last_error


def parse_dataset_names(catalog_xml: str) -> List[str]:
    pattern = r'([A-Za-z0-9_:-]+-\d{10}-B\d{10}-FC\.nc)'

    found = re.findall(pattern, catalog_xml)
    names = []

    for item in found:
        name = str(item).strip().split("/")[-1]
        if re.match(r'^[A-Za-z0-9_:-]+-\d{10}-B\d{10}-FC\.nc$', name):
            names.append(name)

    names = sorted(set(names))

    if not names:
        preview = catalog_xml[:1500].replace("\n", " ")
        raise RuntimeError(
            "No se encontraron datasets tipo *-YYYYMMDDHH-BYYYYMMDDHH-FC.nc "
            f"en el catálogo de agitación. Preview: {preview}"
        )

    return names


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


def get_coord_names(ds: xr.Dataset) -> Tuple[str, str]:
    lon_candidates = ["lon", "longitude", "LON"]
    lat_candidates = ["lat", "latitude", "LAT"]

    lon_name = next((n for n in lon_candidates if n in ds.variables or n in ds.coords), None)
    lat_name = next((n for n in lat_candidates if n in ds.variables or n in ds.coords), None)

    if lon_name is None or lat_name is None:
        raise KeyError("No se encontraron variables/coordenadas lon/lat en el dataset")

    return lon_name, lat_name


def get_coord_arrays(ds: xr.Dataset) -> Tuple[np.ndarray, np.ndarray]:
    lon_name, lat_name = get_coord_names(ds)
    lons = np.asarray(ds[lon_name].values)
    lats = np.asarray(ds[lat_name].values)
    return lons, lats


def build_lonlat_2d(ds: xr.Dataset) -> Tuple[np.ndarray, np.ndarray]:
    lons, lats = get_coord_arrays(ds)

    if lons.ndim == 2 and lats.ndim == 2:
        return lons, lats

    if lons.ndim == 1 and lats.ndim == 1:
        lon2d, lat2d = np.meshgrid(lons, lats)
        return lon2d, lat2d

    raise ValueError(
        f"Dimensiones lon/lat no soportadas: lon.ndim={lons.ndim}, lat.ndim={lats.ndim}"
    )


def get_nearest_indices_port(ds: xr.Dataset, points: List[Dict]) -> List[Tuple[int, int]]:
    lon2d, lat2d = build_lonlat_2d(ds)

    out = []
    for p in points:
        dist2 = (lon2d - p["lon"]) ** 2 + (lat2d - p["lat"]) ** 2

        if np.all(np.isnan(dist2)):
            out.append((None, None))
            continue

        flat_idx = int(np.nanargmin(dist2))
        ilat, ilon = np.unravel_index(flat_idx, dist2.shape)
        out.append((int(ilat), int(ilon)))

    return out


def read_grid_lon_lat(ds: xr.Dataset, ilat: int, ilon: int) -> Tuple[float, float]:
    lon2d, lat2d = build_lonlat_2d(ds)
    return float(lon2d[ilat, ilon]), float(lat2d[ilat, ilon])


def extract_vhm0_value_port(ds: xr.Dataset, ilat: int, ilon: int):
    if "VHM0" not in ds.variables:
        return None

    if ilat is None or ilon is None:
        return None

    da = ds["VHM0"]

    if "time" in da.dims:
        if da.sizes.get("time", 0) == 0:
            return None
        da = da.isel(time=0)

    arr = np.asarray(da.values)
    arr = np.squeeze(arr)

    if arr.ndim != 2:
        raise ValueError(f"VHM0 no es 2D tras squeeze; shape={arr.shape}, dims={da.dims}")

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


def get_port_mesh_inventory() -> Dict[str, Dict]:
    cache_key = f"port_mesh_inventory_v2_past{PAST_DAYS}_future{PORT_FORECAST_DAYS}.json"
    cached = load_json_cache(cache_key)
    if cached:
        return cached

    inventory = {}

    for mesh_key in PORT_MESH_PRIORITY:
        cfg = PORT_MESHES[mesh_key]

        catalog_xml = fetch_catalog_xml(cfg["catalog_xml"])
        dataset_names = parse_dataset_names(catalog_xml)
        start_dt, end_dt = get_window_bounds_for_source(PORT_FORECAST_DAYS)
        files, runs_used, latest_run = select_fc_files_for_window(dataset_names, start_dt, end_dt)

        if not files:
            raise RuntimeError(
                f"La malla portuaria {mesh_key} no tiene ficheros horarios disponibles en la ventana solicitada"
            )

        sample_name = files[0]
        sample_url = cfg["fileserver_base"] + sample_name
        ds, tmp_name = open_local_nc_from_url(sample_url)

        try:
            lon2d, lat2d = build_lonlat_2d(ds)

            inventory[mesh_key] = {
                "key": mesh_key,
                "catalog_xml": cfg["catalog_xml"],
                "fileserver_base": cfg["fileserver_base"],
                "latest_run": latest_run,
                "runs_used": runs_used,
                "files": files,
                "sample_file": sample_name,
                "lon_min": float(np.nanmin(lon2d)),
                "lon_max": float(np.nanmax(lon2d)),
                "lat_min": float(np.nanmin(lat2d)),
                "lat_max": float(np.nanmax(lat2d)),
            }
        finally:
            try:
                ds.close()
            except Exception:
                pass
            remove_file_safely(tmp_name)

    save_json_cache(cache_key, inventory)
    return inventory


def point_in_port_mesh(point: Dict, mesh_meta: Dict) -> bool:
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

        point["port_mesh"] = assigned_mesh

        if assigned_mesh is not None:
            grouped[assigned_mesh].append(point)

    return grouped


def _process_single_port_hour(nc_name: str, url: str, points: List[Dict], nearest_idxs):
    ds = None
    tmp_name = None
    try:
        ds, tmp_name = open_local_nc_from_url(url)

        meta_nc = parse_fc_dataset_name(nc_name)
        valid_time = meta_nc["valid_dt"] if meta_nc else pd.NaT
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
    sample_ds, sample_tmp = open_local_nc_from_url(sample_url)
    try:
        nearest_idxs = get_nearest_indices_port(sample_ds, points)

        for point, (ilat, ilon) in zip(points, nearest_idxs):
            if ilat is None or ilon is None:
                point_meta[point["point_id"]] = {
                    "lon": None,
                    "lat": None,
                    "distance_to_selected_grid_km": None,
                }
                continue

            grid_lon, grid_lat = read_grid_lon_lat(sample_ds, ilat, ilon)

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


def download_port_agitation(points: List[Dict]):
    print("\n" + "=" * 60)
    print("DESCARGA DE AGITACIÓN EN PUERTO")
    print("=" * 60)

    inventory = get_port_mesh_inventory()
    grouped_points = assign_points_to_port_meshes(points, inventory)

    print("Mallas portuarias detectadas:")
    for mesh_key in PORT_MESH_PRIORITY:
        meta = inventory[mesh_key]
        print(
            f" - {mesh_key}: "
            f"lon=[{meta['lon_min']:.3f}, {meta['lon_max']:.3f}] | "
            f"lat=[{meta['lat_min']:.3f}, {meta['lat_max']:.3f}] | "
            f"run=B{meta['latest_run']}"
        )

    out = []

    for mesh_key in PORT_MESH_PRIORITY:
        mesh_points = grouped_points.get(mesh_key, [])
        if not mesh_points:
            continue

        print(f"\nAsignados a {mesh_key}: {[p['name'] for p in mesh_points]}")
        out.extend(download_port_agitation_for_mesh(mesh_points, inventory[mesh_key]))

    assigned_names = {p["name"] for p in out}
    for p in points:
        if p["name"] not in assigned_names:
            out.append({
                "point_id": p["point_id"],
                "name": p["name"],
                "requested_lon": p["lon"],
                "requested_lat": p["lat"],
                "lon": None,
                "lat": None,
                "forecast": [],
                "port_error": "Punto fuera de las mallas portuarias configuradas",
                "port_search_info": {
                    "mesh_key": None,
                    "mesh_sample_file": None,
                    "mesh_latest_run": None,
                    "mesh_runs_used": [],
                    "mesh_lon_min": None,
                    "mesh_lon_max": None,
                    "mesh_lat_min": None,
                    "mesh_lat_max": None,
                    "distance_to_selected_grid_km": None,
                    "valid_count": 0,
                    "total_count": 0,
                    "valid_ratio": 0.0,
                    "attempts_made": 0,
                    "failed_hours_count": 0,
                    "outside_port_coverage": True,
                },
            })

    out.sort(key=lambda x: x["point_id"])
    return out


# =========================
# GUARDADO FINAL
# =========================

def build_port_output(port_points: List[Dict]) -> Dict:
    total_port_records = sum(len(p.get("forecast", [])) for p in port_points)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "past_days": PAST_DAYS,
        "agitacion_forecast_days": PORT_FORECAST_DAYS,
        "agitacion_forecast_hours": PORT_TOTAL_HOURS,
        "points_total": len(port_points),
        "total_agitacion_records": total_port_records,
    }

    return {
        "summary": summary,
        "points": port_points,
    }


def save_output_json(output: Dict, output_file: str):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nOK. Archivo final guardado en: {output_file}")


# =========================
# MAIN
# =========================

def main():
    print("\n🌊 PIPELINE AGITACIÓN PORTUARIA\n")

    if not Path(POINTS_FILE).exists():
        raise FileNotFoundError(f"No existe el archivo de puntos: {POINTS_FILE}")

    all_points = read_points(POINTS_FILE)
    if not all_points:
        raise ValueError("No se encontraron puntos válidos en el archivo de entrada.")

    points = [p for p in all_points if is_puerto_point(p)]

    print(f"Se han leído {len(all_points)} puntos desde {POINTS_FILE}")
    print(f"Puntos filtrados con '_puerto': {len(points)}")
    print(f"Configuración workers agitación: {MAX_WORKERS_PORT}")

    if not points:
        raise ValueError("No hay puntos con '_puerto' en el archivo de entrada.")

    port_points = download_port_agitation(points)
    output = build_port_output(port_points)
    save_output_json(output, PORT_OUTPUT_JSON)

    print("\nProceso completado correctamente.")


if __name__ == "__main__":
    main()
