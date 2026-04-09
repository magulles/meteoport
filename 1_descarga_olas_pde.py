# -*- coding: utf-8 -*-
"""
Pipeline exclusivo para descargar olas desde Puertos del Estado
y generar un único archivo olas_pde.json.

Salida:
- Estructura tipo meteo_points.json
- Un único bloque por punto con forecast horario PDE:
    hs_pde / tp_pde / di_pde
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

# Evita sobre-paralelismo interno de librerías nativas
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
PDE_OUTPUT_JSON = "1_olas_pde.json"

PAST_DAYS = 1
PDE_FORECAST_DAYS = 3
PDE_FORECAST_HOURS = PDE_FORECAST_DAYS * 24
PDE_TOTAL_HOURS = (PAST_DAYS * 24) + PDE_FORECAST_HOURS

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
    "ibi_fallback": {
        "label": "PDE IBI fallback",
        "catalog_xml": "https://opendap.puertos.es/thredds/catalog/wave_regional_ibi/HOURLY/catalog.xml",
        "fileserver_base": "https://opendap.puertos.es/thredds/fileServer/wave_regional_ibi/HOURLY/",
    },
}

PDE_REGION_PRIORITY = ["gib", "bal", "can", "aib", "ibi_fallback"]

DEFAULT_HTTP_HEADERS = {
    "User-Agent": "meteoport/1.0 (+https://github.com/)"
}
RETRYABLE_HTTP_CODES = {500, 502, 503, 504}

MAX_WORKERS_PDE = int(os.getenv("METEOPORT_MAX_WORKERS_PDE", "1"))
HTTP_TIMEOUT_CATALOG = int(os.getenv("METEOPORT_HTTP_TIMEOUT_CATALOG", "60"))
HTTP_TIMEOUT_FILE = int(os.getenv("METEOPORT_HTTP_TIMEOUT_FILE", "120"))

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
# PUERTOS DEL ESTADO
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
            f"en el catálogo de Puertos del Estado. Preview: {preview}"
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


def assign_points_to_pde_regions(points: List[Dict], inventory: Dict[str, Dict]) -> Tuple[Dict[str, List[Dict]], List[Dict]]:
    grouped = {k: [] for k in PDE_REGION_PRIORITY}
    unassigned = []

    for point in points:
        assigned_region = None
        for region_key in PDE_REGION_PRIORITY:
            if point_in_region(point, inventory[region_key]):
                assigned_region = region_key
                break

        point["pde_region"] = assigned_region

        if assigned_region is None:
            unassigned.append(point)
        else:
            grouped[assigned_region].append(point)

    return grouped, unassigned


def _process_single_pde_hour(nc_name: str, url: str, points: List[Dict], nearest_idxs):
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
                result = _process_single_pde_hour(nc_name, url, points, nearest_idxs)
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
                fut = executor.submit(_process_single_pde_hour, nc_name, url, points, nearest_idxs)
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
    grouped_points, unassigned_points = assign_points_to_pde_regions(points, inventory)

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

    if unassigned_points:
        print("\nPuntos fuera de cobertura PDE:")
        for p in unassigned_points:
            print(f" - {p['name']} ({p['lon']}, {p['lat']})")

            out.append({
                "point_id": p["point_id"],
                "name": p["name"],
                "requested_lon": p["lon"],
                "requested_lat": p["lat"],
                "lon": None,
                "lat": None,
                "forecast": [],
                "pde_error": "Punto fuera de las mallas regionales de Puertos del Estado",
                "pde_search_info": {
                    "region_key": None,
                    "region_label": None,
                    "region_sample_file": None,
                    "region_latest_run": None,
                    "region_runs_used": [],
                    "region_lon_min": None,
                    "region_lon_max": None,
                    "region_lat_min": None,
                    "region_lat_max": None,
                    "distance_to_selected_grid_km": None,
                    "valid_count": 0,
                    "total_count": 0,
                    "valid_ratio": 0.0,
                    "attempts_made": 0,
                    "failed_hours_count": 0,
                    "outside_pde_coverage": True,
                },
            })

    out.sort(key=lambda x: x["point_id"])
    return out


# =========================
# GUARDADO FINAL
# =========================

def build_pde_output(pde_points: List[Dict]) -> Dict:
    total_pde_records = sum(len(p.get("forecast", [])) for p in pde_points)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "past_days": PAST_DAYS,
        "pde_forecast_days": PDE_FORECAST_DAYS,
        "pde_forecast_hours": PDE_TOTAL_HOURS,
        "points_total": len(pde_points),
        "total_pde_records": total_pde_records,
    }

    return {
        "summary": summary,
        "points": pde_points,
    }


def save_output_json(output: Dict, output_file: str):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nOK. Archivo final guardado en: {output_file}")


# =========================
# MAIN
# =========================

def main():
    print("\n🌊 PIPELINE PDE: OLAS PUERTOS DEL ESTADO\n")

    if not Path(POINTS_FILE).exists():
        raise FileNotFoundError(f"No existe el archivo de puntos: {POINTS_FILE}")

    points = read_points(POINTS_FILE)
    if not points:
        raise ValueError("No se encontraron puntos válidos en el archivo de entrada.")

    print(f"Se han leído {len(points)} puntos desde {POINTS_FILE}")
    print(f"Configuración workers PDE: {MAX_WORKERS_PDE}")

    pde_points = download_pde_wave_data(points)
    output = build_pde_output(pde_points)
    save_output_json(output, PDE_OUTPUT_JSON)

    print("\nProceso completado correctamente.")


if __name__ == "__main__":
    main()
