# -*- coding: utf-8 -*-

"""
meteo_pipeline.py

Pipeline único para:
1) descargar olas desde Copernicus Marine
2) descargar viento 10 m desde Open-Meteo
3) fusionar ambos resultados por punto y tiempo
4) generar:
   - wave_points.json
   - wind_10m_forecast.json
   - meteo_points.json

Mejora:
- Si un punto de olas devuelve toda la serie nula, busca automáticamente
  una celda vecina válida alrededor del punto solicitado.
"""

import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import copernicusmarine
import numpy as np
import pandas as pd
import requests
import xarray as xr


# =========================
# CONFIGURACIÓN GENERAL
# =========================

POINTS_FILE = "lonp_latp.txt"

WAVE_OUTPUT_JSON = "wave_points.json"
WIND_OUTPUT_JSON = "wind_10m_forecast.json"
METEO_OUTPUT_JSON = "meteo_points.json"

FORECAST_DAYS = 3

# --- Copernicus olas ---
DATASET_ID = "cmems_mod_ibi_wav_anfc_0.027deg_PT1H-i"

USERNAME = os.environ["COPERNICUS_USERNAME"]
PASSWORD = os.environ["COPERNICUS_PASSWORD"]

# --- Open-Meteo viento ---
BASE_URL = "https://api.open-meteo.com/v1/forecast"

# --- Merge ---
KEEP_ONLY_COMMON_TIMES = True

# --- Búsqueda vecina para olas ---
# Desplazamientos en grados a probar alrededor del punto original
SEARCH_OFFSETS = [0.0, 0.02, 0.04, 0.06, 0.08]
MIN_VALID_RATIO = 0.30  # fracción mínima de registros válidos para aceptar una celda


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


def read_points(filename):
    points = []

    with open(filename, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()

            if not line:
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
                "name": str(name).strip(),
                "lon": lon,
                "lat": lat
            })

    return points


def temp_nc_name(point_id, attempt_idx):
    return f"wave_point_{point_id:03d}_try_{attempt_idx:02d}.nc"


def offsets_to_try():
    """
    Genera desplazamientos ordenados por distancia al punto original.
    Empieza por (0,0), luego cruces, diagonales y radios crecientes.
    """
    combos = set()
    combos.add((0.0, 0.0))

    for d in SEARCH_OFFSETS:
        if d == 0:
            continue
        basic = [
            ( d,  0.0), (-d,  0.0), (0.0,  d), (0.0, -d),
            ( d,  d),   ( d, -d),   (-d, d),   (-d, -d)
        ]
        for item in basic:
            combos.add(item)

    return sorted(combos, key=lambda xy: (xy[0] ** 2 + xy[1] ** 2, abs(xy[0]), abs(xy[1])))


def haversine_km(lon1, lat1, lon2, lat2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)

    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1 - a)))
    return r * c


# =========================
# 1) DESCARGA DE OLAS
# =========================

def fetch_wave_candidate(req_lon, req_lat, point_id, attempt_idx, start_datetime, end_datetime):
    outfile = temp_nc_name(point_id, attempt_idx)

    copernicusmarine.subset(
        dataset_id=DATASET_ID,
        username=USERNAME,
        password=PASSWORD,
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
                "di": di_v
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
            "temp_nc_file": outfile
        }

    finally:
        ds.close()


def pick_best_wave_candidate(candidates):
    if not candidates:
        return None

    return sorted(
        candidates,
        key=lambda c: (
            c["valid_ratio"],
            c["valid_count"],
            -(c["distance_to_selected_grid_km"] or 0.0)
        ),
        reverse=True
    )[0]


def cleanup_temp_files(files):
    for ncfile in files:
        try:
            if ncfile and os.path.exists(ncfile):
                os.remove(ncfile)
                print(f"Borrado temporal: {ncfile}")
        except Exception as e:
            print(f"No se pudo borrar {ncfile}: {e}")


def download_wave_data(points):
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = today + timedelta(days=FORECAST_DAYS)

    start_datetime = today.strftime("%Y-%m-%dT%H:%M:%S")
    end_datetime = end_day.strftime("%Y-%m-%dT%H:%M:%S")

    print("\n" + "=" * 60)
    print("DESCARGA DE OLAS")
    print("=" * 60)
    print(f"start_datetime = {start_datetime}")
    print(f"end_datetime   = {end_datetime}")

    all_points_data = []
    all_temp_files = []
    search_plan = offsets_to_try()

    for i, point in enumerate(points, start=1):
        point_id = i
        name = point["name"]
        base_lon = point["lon"]
        base_lat = point["lat"]

        print(f"\n[PUNTO {point_id}] {name} | lon={base_lon}, lat={base_lat}")

        candidates = []
        errors = []

        for attempt_idx, (dlon, dlat) in enumerate(search_plan, start=1):
            req_lon = base_lon + dlon
            req_lat = base_lat + dlat

            try:
                candidate = fetch_wave_candidate(
                    req_lon=req_lon,
                    req_lat=req_lat,
                    point_id=point_id,
                    attempt_idx=attempt_idx,
                    start_datetime=start_datetime,
                    end_datetime=end_datetime
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

        best = pick_best_wave_candidate(candidates)

        if best is None:
            all_points_data.append({
                "point_id": point_id,
                "name": name,
                "requested_lon": base_lon,
                "requested_lat": base_lat,
                "lon": None,
                "lat": None,
                "forecast": [],
                "error": "No se pudo obtener ningún candidato de olas",
                "search_errors": errors
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
                "min_valid_ratio_target": MIN_VALID_RATIO
            }
        }

        if errors:
            point_data["wave_search_errors"] = errors

        all_points_data.append(point_data)

    json_written = False

    try:
        with open(WAVE_OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(all_points_data, f, ensure_ascii=False, indent=2)

        json_written = True
        print(f"\nJSON de olas generado correctamente: {WAVE_OUTPUT_JSON}")
        print(f"Puntos guardados: {len(all_points_data)}")

    except Exception as e:
        print(f"\nERROR al escribir {WAVE_OUTPUT_JSON}: {e}")

    if json_written:
        cleanup_temp_files(all_temp_files)
    else:
        print("\nEl JSON de olas no se generó correctamente. Se conservan los .nc temporales para revisión.")

    return all_points_data


# =========================
# 2) DESCARGA DE VIENTO
# =========================

import time
import requests

def fetch_wind_forecast(lat, lon, max_retries=3, timeout=60):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "wind_speed_10m,wind_direction_10m",
        "wind_speed_unit": "ms",
        "forecast_days": FORECAST_DAYS,
        "timezone": "UTC"
    }

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=timeout)
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
    print("DESCARGA DE VIENTO")
    print("=" * 60)

    all_data = []

    for i, point in enumerate(points, start=1):
        print(f"Descargando viento punto {i}/{len(points)}: {point['name']} ({point['lon']}, {point['lat']})")

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
                    "wind_direction_10m_deg": round_or_none(d, 2)
                })

            all_data.append({
                "name": point["name"],
                "lon": point["lon"],
                "lat": point["lat"],
                "forecast": records
            })

            print(f"OK leído viento punto {i} ({point['name']}): {len(records)} registros")

        except requests.RequestException as e:
            print(f"[ERROR] No se pudo descargar viento para {point['name']}: {e}")
            all_data.append({
                "name": point["name"],
                "lon": point["lon"],
                "lat": point["lat"],
                "forecast": [],
                "error": str(e)
            })

    with open(WIND_OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"\nOK. Archivo de viento guardado en: {WIND_OUTPUT_JSON}")

    return all_data


# =========================
# 3) FUSIÓN OLAS + VIENTO
# =========================

def build_forecast_index(records):
    idx = {}
    for rec in records or []:
        t = normalize_time_to_utc_z(rec.get("time"))
        if t:
            idx[t] = rec
    return idx


def merge_wave_and_wind(wave_points, wind_points):
    print("\n" + "=" * 60)
    print("FUSIÓN DE OLAS Y VIENTO")
    print("=" * 60)

    wind_by_name = {str(p.get("name", "")).strip(): p for p in wind_points}
    merged_points = []

    total_wave_records = 0
    total_wind_records = 0
    total_common_records = 0

    for point_id, wave_point in enumerate(wave_points, start=1):
        name = str(wave_point.get("name", "")).strip()
        wind_point = wind_by_name.get(name)

        wave_forecast = wave_point.get("forecast", [])
        wind_forecast = wind_point.get("forecast", []) if wind_point else []

        wave_idx = build_forecast_index(wave_forecast)
        wind_idx = build_forecast_index(wind_forecast)

        wave_times = set(wave_idx.keys())
        wind_times = set(wind_idx.keys())

        total_wave_records += len(wave_times)
        total_wind_records += len(wind_times)

        if KEEP_ONLY_COMMON_TIMES:
            selected_times = sorted(wave_times & wind_times)
        else:
            selected_times = sorted(wave_times | wind_times)

        total_common_records += len(wave_times & wind_times)

        merged_forecast = []
        for t in selected_times:
            wv = wave_idx.get(t, {})
            wd = wind_idx.get(t, {})

            merged_forecast.append({
                "time": t,
                "hs": wv.get("hs"),
                "tp": wv.get("tp"),
                "di": wv.get("di"),
                "wind_speed_10m_ms": wd.get("wind_speed_10m_ms"),
                "wind_direction_10m_deg": wd.get("wind_direction_10m_deg")
            })

        merged_point = {
            "point_id": wave_point.get("point_id", point_id),
            "name": name,
            "requested_lon": wave_point.get("requested_lon"),
            "requested_lat": wave_point.get("requested_lat"),
            "lon": wave_point.get("lon", wave_point.get("requested_lon")),
            "lat": wave_point.get("lat", wave_point.get("requested_lat")),
            "forecast": merged_forecast,
            "merge_info": {
                "wave_records": len(wave_times),
                "wind_records": len(wind_times),
                "common_records": len(wave_times & wind_times),
                "wave_only_records": len(wave_times - wind_times),
                "wind_only_records": len(wind_times - wave_times),
                "missing_wind_point": wind_point is None
            }
        }

        if "wave_search_info" in wave_point:
            merged_point["wave_search_info"] = wave_point["wave_search_info"]
        if "wave_search_errors" in wave_point:
            merged_point["wave_search_errors"] = wave_point["wave_search_errors"]

        if "error" in wave_point:
            merged_point["wave_error"] = wave_point["error"]
        if wind_point and "error" in wind_point:
            merged_point["wind_error"] = wind_point["error"]
        if wind_point is None:
            merged_point["wind_error"] = f"No existe punto de viento con name='{name}'"

        merged_points.append(merged_point)

        print(
            f"Punto {merged_point['point_id']} ({name}): "
            f"olas={len(wave_times)}, viento={len(wind_times)}, comunes={len(wave_times & wind_times)}"
        )

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forecast_days": FORECAST_DAYS,
        "keep_only_common_times": KEEP_ONLY_COMMON_TIMES,
        "points_total": len(merged_points),
        "total_wave_records": total_wave_records,
        "total_wind_records": total_wind_records,
        "total_common_records": total_common_records
    }

    output = {
        "summary": summary,
        "points": merged_points
    }

    with open(METEO_OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nOK. Archivo final guardado en: {METEO_OUTPUT_JSON}")

    return output


# =========================
# MAIN
# =========================

def main():
    print("\n🌊💨 METEO PIPELINE: OLAS + VIENTO\n")

    if not Path(POINTS_FILE).exists():
        raise FileNotFoundError(f"No existe el archivo de puntos: {POINTS_FILE}")

    points = read_points(POINTS_FILE)

    if not points:
        raise ValueError("No se encontraron puntos válidos en el archivo de entrada.")

    print(f"Se han leído {len(points)} puntos desde {POINTS_FILE}")

    wave_points = download_wave_data(points)
    wind_points = download_wind_data(points)
    merge_wave_and_wind(wave_points, wind_points)

    print("\nProceso completado correctamente.")


if __name__ == "__main__":
    main()
