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
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

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
COPERNICUS_FORECAST_DAYS = 5
OPEN_METEO_FORECAST_DAYS = 5
PDE_FORECAST_DAYS = 3
PDE_FORECAST_HOURS = PDE_FORECAST_DAYS * 24

# --- Copernicus olas ---
COPERNICUS_DATASET_ID = "cmems_mod_ibi_wav_anfc_0.027deg_PT1H-i"
COPERNICUS_USERNAME = os.environ["COPERNICUS_USERNAME"]
COPERNICUS_PASSWORD = os.environ["COPERNICUS_PASSWORD"]
COPERNICUS_SEARCH_OFFSETS = [0.0, 0.02, 0.04, 0.06, 0.08]
MIN_VALID_RATIO = 0.30

# --- Open-Meteo viento ---
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# --- Puertos del Estado ---
PDE_CATALOG_XML = "https://opendap.puertos.es/thredds/catalog/wave_regional_ibi/HOURLY/catalog.xml"
PDE_FILESERVER_BASE = "https://opendap.puertos.es/thredds/fileServer/wave_regional_ibi/HOURLY/"
PDE_NEIGHBOR_RADIUS = 3

# --- HTTP general / robustez ---
DEFAULT_HTTP_HEADERS = {
    "User-Agent": "meteoport/1.0 (+https://github.com/)"
}
RETRYABLE_HTTP_CODES = {500, 502, 503, 504}


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

    ds = xr.open_dataset(outfile)

    try:
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
    finally:
        ds.close()



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



def download_copernicus_wave_data(points):
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    end_datetime_dt = today + timedelta(hours=(COPERNICUS_FORECAST_DAYS * 24) - 1)

    start_datetime = today.strftime("%Y-%m-%dT%H:%M:%S")
    end_datetime = end_datetime_dt.strftime("%Y-%m-%dT%H:%M:%S")

    print("\n" + "=" * 60)
    print("DESCARGA DE OLAS COPERNICUS")
    print("=" * 60)
    print(f"start_datetime = {start_datetime}")
    print(f"end_datetime   = {end_datetime}")

    all_points_data = []
    all_temp_files = []
    search_plan = offsets_to_try()

    for point in points:
        point_id = point["point_id"]
        name = point["name"]
        base_lon = point["lon"]
        base_lat = point["lat"]

        print(f"\n[COPERNICUS PUNTO {point_id}] {name} | lon={base_lon}, lat={base_lat}")

        candidates = []
        errors = []

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

                all_temp_files.append(candidate["temp_nc_file"])
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
            all_points_data.append({
                "point_id": point_id,
                "name": name,
                "requested_lon": base_lon,
                "requested_lat": base_lat,
                "lon": None,
                "lat": None,
                "forecast": [],
                "error": "No se pudo obtener ningún candidato de olas Copernicus",
                "search_errors": errors,
            })
            continue

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

        all_points_data.append(point_data)

    cleanup_temp_files(all_temp_files)
    return all_points_data


# =========================
# 2) OPEN-METEO
# =========================


def fetch_wind_forecast(lat, lon, max_retries=3, timeout=60):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "wind_speed_10m,wind_direction_10m",
        "wind_speed_unit": "ms",
        "forecast_days": COPERNICUS_FORECAST_DAYS,
        "timezone": "UTC",
    }

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
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



def download_wind_data(points):
    print("\n" + "=" * 60)
    print("DESCARGA DE VIENTO OPEN-METEO")
    print("=" * 60)

    all_data = []

    for point in points:
        print(
            f"Descargando viento punto {point['point_id']}/{len(points)}: "
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

            all_data.append({
                "point_id": point["point_id"],
                "name": point["name"],
                "lon": point["lon"],
                "lat": point["lat"],
                "forecast": records,
            })

            print(f"OK leído viento punto {point['point_id']} ({point['name']}): {len(records)} registros")

        except requests.RequestException as e:
            print(f"[ERROR] No se pudo descargar viento para {point['name']}: {e}")
            all_data.append({
                "point_id": point["point_id"],
                "name": point["name"],
                "lon": point["lon"],
                "lat": point["lat"],
                "forecast": [],
                "error": str(e),
            })

    return all_data


# =========================
# 3) PUERTOS DEL ESTADO
# =========================


def fetch_catalog_xml(catalog_url: str = PDE_CATALOG_XML, timeout=60, max_retries=4) -> str:
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(catalog_url, timeout=timeout, headers=DEFAULT_HTTP_HEADERS)
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



def open_local_nc_from_url(url: str, timeout=120, max_retries=5, backoff=8):
    last_error = None

    for attempt in range(1, max_retries + 1):
        tmp_name = None

        try:
            with requests.get(url, stream=True, timeout=timeout, headers=DEFAULT_HTTP_HEADERS) as r:
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
                ds = xr.open_dataset(tmp_name, engine="netcdf4")
            except Exception:
                ds = xr.open_dataset(tmp_name, engine="h5netcdf")

            return ds, tmp_name

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



def build_pde_candidate_offsets(radius=PDE_NEIGHBOR_RADIUS):
    offsets = []
    for di in range(-radius, radius + 1):
        for dj in range(-radius, radius + 1):
            offsets.append((di, dj))

    offsets.sort(key=lambda ij: (ij[0] ** 2 + ij[1] ** 2, abs(ij[0]), abs(ij[1])))
    return offsets



def get_nearest_indices(ds: xr.Dataset, points: List[Dict]) -> List[Tuple[int, int]]:
    lats = ds["latitude"].values
    lons = ds["longitude"].values

    idxs = []
    for point in points:
        ilat = int(np.abs(lats - point["lat"]).argmin())
        ilon = int(np.abs(lons - point["lon"]).argmin())
        idxs.append((ilat, ilon))
    return idxs



def extract_pde_value(ds: xr.Dataset, var_name: str, ilat: int, ilon: int):
    if var_name not in ds.variables:
        return None
    try:
        return round_or_none(ds[var_name].isel(time=0, latitude=ilat, longitude=ilon).values, 2)
    except Exception:
        return None



def find_valid_pde_cell(ds: xr.Dataset, point: Dict, base_ilat: int, base_ilon: int, offsets):
    lats = ds["latitude"].values
    lons = ds["longitude"].values
    nlat = len(lats)
    nlon = len(lons)

    for dlat_idx, dlon_idx in offsets:
        ilat = base_ilat + dlat_idx
        ilon = base_ilon + dlon_idx

        if ilat < 0 or ilat >= nlat or ilon < 0 or ilon >= nlon:
            continue

        hs = extract_pde_value(ds, "VHM0", ilat, ilon)
        tp = extract_pde_value(ds, "VTPK", ilat, ilon)
        di = extract_pde_value(ds, "VMDR", ilat, ilon)

        if hs is None and tp is None and di is None:
            continue

        grid_lon = float(lons[ilon])
        grid_lat = float(lats[ilat])

        return {
            "ilat": ilat,
            "ilon": ilon,
            "lon": grid_lon,
            "lat": grid_lat,
            "distance_to_selected_grid_km": round(
                haversine_km(point["lon"], point["lat"], grid_lon, grid_lat), 3
            ),
            "grid_offset_i": int(dlat_idx),
            "grid_offset_j": int(dlon_idx),
        }

    return None



def download_pde_wave_data(points):
    print("\n" + "=" * 60)
    print("DESCARGA DE OLAS PUERTOS DEL ESTADO")
    print("=" * 60)

    catalog_xml = fetch_catalog_xml()
    dataset_names = parse_dataset_names(catalog_xml)
    latest_run, files = choose_latest_run(dataset_names)
    files = files[:PDE_FORECAST_HOURS]

    print(f"Run PDE más reciente detectado: B{latest_run}")
    print(f"Número de ficheros horarios a procesar: {len(files)}")

    nearest_idxs = None
    pde_offsets = build_pde_candidate_offsets()

    point_meta = {
        p["point_id"]: {
            "selected_lon": None,
            "selected_lat": None,
            "distance_to_selected_grid_km": None,
            "grid_offset_i": None,
            "grid_offset_j": None,
            "search_radius_cells": PDE_NEIGHBOR_RADIUS,
            "selection_locked": False,
            "selection_source_file": None,
        }
        for p in points
    }

    point_fixed_idxs = {p["point_id"]: None for p in points}
    point_selection_attempts = {p["point_id"]: 0 for p in points}

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

    temp_files = []
    failed_hours = []

    for k, nc_name in enumerate(files, start=1):
        url = PDE_FILESERVER_BASE + nc_name
        print(f"[{k}/{len(files)}] Descargando y procesando {nc_name}", flush=True)

        try:
            ds, tmp_name = open_local_nc_from_url(url)
            temp_files.append(tmp_name)

            try:
                if nearest_idxs is None:
                    nearest_idxs = get_nearest_indices(ds, points)

                m = re.match(r"HW-(\d{10})-B(\d{10})-FC\.nc", nc_name)
                valid_time = pd.to_datetime(m.group(1), format="%Y%m%d%H", utc=True) if m else pd.NaT
                valid_time_str = valid_time.strftime("%Y-%m-%dT%H:%M:%SZ") if not pd.isna(valid_time) else None

                for point, (base_ilat, base_ilon) in zip(points, nearest_idxs):
                    pid = point["point_id"]
                    rec = {"time": valid_time_str}

                    fixed_idx = point_fixed_idxs[pid]
                    if fixed_idx is None:
                        point_selection_attempts[pid] += 1
                        selected = find_valid_pde_cell(
                            ds=ds,
                            point=point,
                            base_ilat=base_ilat,
                            base_ilon=base_ilon,
                            offsets=pde_offsets,
                        )

                        if selected is not None:
                            point_fixed_idxs[pid] = (selected["ilat"], selected["ilon"])
                            meta = point_meta[pid]
                            meta["selected_lon"] = selected["lon"]
                            meta["selected_lat"] = selected["lat"]
                            meta["distance_to_selected_grid_km"] = selected["distance_to_selected_grid_km"]
                            meta["grid_offset_i"] = selected["grid_offset_i"]
                            meta["grid_offset_j"] = selected["grid_offset_j"]
                            meta["selection_locked"] = True
                            meta["selection_source_file"] = nc_name
                            fixed_idx = point_fixed_idxs[pid]

                    if fixed_idx is None:
                        rec["hs_pde"] = None
                        rec["tp_pde"] = None
                        rec["di_pde"] = None
                    else:
                        ilat, ilon = fixed_idx
                        rec["hs_pde"] = extract_pde_value(ds, "VHM0", ilat, ilon)
                        rec["tp_pde"] = extract_pde_value(ds, "VTPK", ilat, ilon)
                        rec["di_pde"] = extract_pde_value(ds, "VMDR", ilat, ilon)

                    point_forecasts[pid]["forecast"].append(rec)

            finally:
                try:
                    ds.close()
                except Exception:
                    pass

        except Exception as e:
            print(f"    ERROR en {nc_name}: {e}", flush=True)
            failed_hours.append({
                "file": nc_name,
                "url": url,
                "error": str(e),
            })
            continue

    cleanup_temp_files(temp_files)

    out = []
    for point in points:
        pid = point["point_id"]
        meta = point_meta.get(pid, {})
        forecast = point_forecasts[pid]["forecast"]
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
            "lon": meta.get("selected_lon"),
            "lat": meta.get("selected_lat"),
            "forecast": forecast,
            "pde_search_info": {
                "distance_to_selected_grid_km": meta.get("distance_to_selected_grid_km"),
                "valid_count": valid_count,
                "total_count": total_count,
                "valid_ratio": round(valid_count / total_count, 4) if total_count else 0.0,
                "attempts_made": point_selection_attempts[pid],
                "failed_hours_count": len(failed_hours),
                "search_radius_cells": meta.get("search_radius_cells"),
                "grid_offset_i": meta.get("grid_offset_i"),
                "grid_offset_j": meta.get("grid_offset_j"),
                "selection_locked": meta.get("selection_locked", False),
                "selection_source_file": meta.get("selection_source_file"),
            },
        }

        if failed_hours:
            point_out["pde_failed_hours"] = failed_hours

        out.append(point_out)

    if failed_hours:
        print("\n[AVISO] Horas PDE no descargadas:", flush=True)
        for item in failed_hours:
            print(f" - {item['file']} -> {item['error']}", flush=True)
        print("Se continúa el pipeline y esas horas quedarán como faltantes en PDE.", flush=True)

    return out


# =========================
# 4) FUSIÓN FINAL
# =========================


def merge_all_sources(copernicus_points, pde_points, wind_points):
    print("\n" + "=" * 60)
    print("FUSIÓN FINAL: COPERNICUS + PDE + VIENTO")
    print("=" * 60)

    cop_by_name = {str(p.get("name", "")).strip(): p for p in copernicus_points}
    pde_by_name = {str(p.get("name", "")).strip(): p for p in pde_points}
    wind_by_name = {str(p.get("name", "")).strip(): p for p in wind_points}

    merged_points = []
    all_names = sorted(set(cop_by_name.keys()) | set(pde_by_name.keys()) | set(wind_by_name.keys()))

    total_cop_records = 0
    total_pde_records = 0
    total_wind_records = 0
    total_common_records = 0

    for idx, name in enumerate(all_names, start=1):
        cop = cop_by_name.get(name)
        pde = pde_by_name.get(name)
        wind = wind_by_name.get(name)

        cop_forecast = cop.get("forecast", []) if cop else []
        pde_forecast = pde.get("forecast", []) if pde else []
        wind_forecast = wind.get("forecast", []) if wind else []

        cop_idx = build_forecast_index(cop_forecast)
        pde_idx = build_forecast_index(pde_forecast)
        wind_idx = build_forecast_index(wind_forecast)

        cop_times = set(cop_idx.keys())
        pde_times = set(pde_idx.keys())
        wind_times = set(wind_idx.keys())

        total_cop_records += len(cop_times)
        total_pde_records += len(pde_times)
        total_wind_records += len(wind_times)

        if KEEP_ONLY_COMMON_TIMES:
            selected_times = sorted(cop_times & pde_times & wind_times)
        else:
            selected_times = sorted(cop_times | pde_times | wind_times)

        total_common_records += len(cop_times & pde_times & wind_times)

        merged_forecast = []
        for t in selected_times:
            c = cop_idx.get(t, {})
            p = pde_idx.get(t, {})
            w = wind_idx.get(t, {})

            merged_forecast.append({
                "time": t,
                "hs": c.get("hs"),
                "tp": c.get("tp"),
                "di": c.get("di"),
                "hs_pde": p.get("hs_pde"),
                "tp_pde": p.get("tp_pde"),
                "di_pde": p.get("di_pde"),
                "wind_speed_10m_ms": w.get("wind_speed_10m_ms"),
                "wind_direction_10m_deg": w.get("wind_direction_10m_deg"),
            })

        base = cop or pde or wind or {}
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
                "common_records": len(cop_times & pde_times & wind_times),
                "selected_records": len(selected_times),
                "copernicus_only_records": len(cop_times - pde_times - wind_times),
                "pde_only_records": len(pde_times - cop_times - wind_times),
                "wind_only_records": len(wind_times - cop_times - pde_times),
                "missing_copernicus_point": cop is None,
                "missing_pde_point": pde is None,
                "missing_wind_point": wind is None,
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
            f"comunes={len(cop_times & pde_times & wind_times)}"
        )

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "copernicus_forecast_days": COPERNICUS_FORECAST_DAYS,
        "copernicus_forecast_hours": COPERNICUS_FORECAST_DAYS * 24,
        "open_meteo_forecast_days": OPEN_METEO_FORECAST_DAYS,
        "open_meteo_forecast_hours": OPEN_METEO_FORECAST_DAYS * 24,
        "pde_forecast_days": PDE_FORECAST_DAYS,
        "pde_forecast_hours": PDE_FORECAST_HOURS,
        "keep_only_common_times": KEEP_ONLY_COMMON_TIMES,
        "points_total": len(merged_points),
        "total_copernicus_records": total_cop_records,
        "total_pde_records": total_pde_records,
        "total_wind_records": total_wind_records,
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
    print("\n🌊🌬️ METEO PIPELINE FUSIONADO: COPERNICUS + PDE + VIENTO\n")

    if not Path(POINTS_FILE).exists():
        raise FileNotFoundError(f"No existe el archivo de puntos: {POINTS_FILE}")

    points = read_points(POINTS_FILE)
    if not points:
        raise ValueError("No se encontraron puntos válidos en el archivo de entrada.")

    print(f"Se han leído {len(points)} puntos desde {POINTS_FILE}")

    copernicus_points = download_copernicus_wave_data(points)
    pde_points = download_pde_wave_data(points)
    wind_points = download_wind_data(points)
    merge_all_sources(copernicus_points, pde_points, wind_points)

    print("\nProceso completado correctamente.")


if __name__ == "__main__":
    main()
