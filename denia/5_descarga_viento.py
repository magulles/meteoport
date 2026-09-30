# -*- coding: utf-8 -*-
"""
Pipeline exclusivo para descargar viento 10 m
y generar un único archivo viento_openmeteo.json.

Salida:
- Estructura tipo meteo_points.json
- Un único bloque por punto con forecast horario de viento:
    wind_speed_10m_ms / wind_direction_10m_deg
"""

from __future__ import annotations

import json
import os
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
from typing import Dict, List

import numpy as np
import pandas as pd
import requests


# =========================
# CONFIGURACIÓN GENERAL
# =========================

POINTS_FILE = "lonp_latp.txt"
WIND_OUTPUT_JSON = "5_viento.json"

PAST_DAYS = 1
OPEN_METEO_FORECAST_DAYS = 3
OPEN_METEO_TOTAL_HOURS = (PAST_DAYS + OPEN_METEO_FORECAST_DAYS) * 24

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPENWEATHER_URL = "https://api.openweathermap.org/data/3.0/onecall"
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "").strip()

DEFAULT_HTTP_HEADERS = {
    "User-Agent": "meteoport/1.0 (+https://github.com/)"
}
RETRYABLE_HTTP_CODES = {500, 502, 503, 504}

MAX_WORKERS_WIND = int(os.getenv("METEOPORT_MAX_WORKERS_WIND", "8"))
HTTP_TIMEOUT_WIND = int(os.getenv("METEOPORT_HTTP_TIMEOUT_WIND", "60"))

SESSION_POOL_CONNECTIONS = int(os.getenv("METEOPORT_SESSION_POOL_CONNECTIONS", "32"))
SESSION_POOL_MAXSIZE = int(os.getenv("METEOPORT_SESSION_POOL_MAXSIZE", "32"))


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


# =========================
# VIENTO
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
            data = response.json()
            data["_wind_source"] = "open-meteo"
            return data

        except requests.RequestException as e:
            last_error = e
            print(f"  [reintento {attempt}/{max_retries}] error viento Open-Meteo lat={lat}, lon={lon}: {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt)

    if not OPENWEATHER_API_KEY:
        raise RuntimeError(
            f"Open-Meteo falló para lat={lat}, lon={lon} y no hay OPENWEATHER_API_KEY configurada"
        ) from last_error

    print(f"  [fallback] Open-Meteo falló para lat={lat}, lon={lon}. Probando OpenWeather...")

    try:
        params_ow = {
            "lat": lat,
            "lon": lon,
            "exclude": "minutely,daily",
            "appid": OPENWEATHER_API_KEY,
            "units": "metric",
        }

        response = get_requests_session().get(
            OPENWEATHER_URL,
            params=params_ow,
            timeout=timeout,
            headers=DEFAULT_HTTP_HEADERS,
        )
        response.raise_for_status()
        data = response.json()
        data["_wind_source"] = "openweather"
        return data

    except requests.RequestException as e:
        print(f"  [fallback ERROR] OpenWeather también falló lat={lat}, lon={lon}: {e}")
        raise e from last_error


def _download_single_wind_point(point, total_points):
    print(
        f"Descargando viento punto {point['point_id']}/{total_points}: "
        f"{point['name']} ({point['lon']}, {point['lat']})"
    )

    try:
        data = fetch_wind_forecast(point["lat"], point["lon"])
        source = data.get("_wind_source", "unknown")

        records = []

        if source == "open-meteo":
            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            speeds = hourly.get("wind_speed_10m", [])
            directions = hourly.get("wind_direction_10m", [])

            for t, s, d in zip(times, speeds, directions):
                records.append({
                    "time": normalize_time_to_utc_z(t),
                    "wind_speed_10m_ms": round_or_none(s, 2),
                    "wind_direction_10m_deg": round_or_none(d, 2),
                })

        elif source == "openweather":
            hourly = data.get("hourly", [])

            for row in hourly:
                t = datetime.fromtimestamp(row["dt"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                records.append({
                    "time": t,
                    "wind_speed_10m_ms": round_or_none(row.get("wind_speed"), 2),
                    "wind_direction_10m_deg": round_or_none(row.get("wind_deg"), 2),
                })

        print(f"OK leído viento punto {point['point_id']} ({point['name']}): {len(records)} registros [{source}]")
        return {
            "point_id": point["point_id"],
            "name": point["name"],
            "requested_lon": point["lon"],
            "requested_lat": point["lat"],
            "lon": point["lon"],
            "lat": point["lat"],
            "forecast": records,
            "wind_source": source,
        }

    except Exception as e:
        print(f"[ERROR] No se pudo descargar viento para {point['name']}: {e}")
        return {
            "point_id": point["point_id"],
            "name": point["name"],
            "requested_lon": point["lon"],
            "requested_lat": point["lat"],
            "lon": point["lon"],
            "lat": point["lat"],
            "forecast": [],
            "wind_error": str(e),
        }


def download_wind_data(points):
    print("\n" + "=" * 60)
    print("DESCARGA DE VIENTO OPEN-METEO / OPENWEATHER")
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
# GUARDADO FINAL
# =========================

def build_wind_output(wind_points: List[Dict]) -> Dict:
    total_wind_records = sum(len(p.get("forecast", [])) for p in wind_points)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "past_days": PAST_DAYS,
        "open_meteo_forecast_days": OPEN_METEO_FORECAST_DAYS,
        "open_meteo_forecast_hours": OPEN_METEO_TOTAL_HOURS,
        "points_total": len(wind_points),
        "total_wind_records": total_wind_records,
    }

    return {
        "summary": summary,
        "points": wind_points,
    }


def save_output_json(output: Dict, output_file: str):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nOK. Archivo final guardado en: {output_file}")


# =========================
# MAIN
# =========================

def main():
    print("\n🌬️ PIPELINE VIENTO OPEN-METEO / OPENWEATHER\n")

    if not Path(POINTS_FILE).exists():
        raise FileNotFoundError(f"No existe el archivo de puntos: {POINTS_FILE}")

    points = read_points(POINTS_FILE)
    if not points:
        raise ValueError("No se encontraron puntos válidos en el archivo de entrada.")

    print(f"Se han leído {len(points)} puntos desde {POINTS_FILE}")
    print(f"Configuración workers viento: {MAX_WORKERS_WIND}")

    wind_points = download_wind_data(points)
    output = build_wind_output(wind_points)
    save_output_json(output, WIND_OUTPUT_JSON)

    print("\nProceso completado correctamente.")


if __name__ == "__main__":
    main()
