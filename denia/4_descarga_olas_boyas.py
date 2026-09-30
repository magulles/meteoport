# -*- coding: utf-8 -*-

import os
import json
import shutil
from pathlib import Path
from datetime import datetime, timedelta, timezone

import copernicusmarine
import numpy as np
import pandas as pd
import xarray as xr


# =========================
# CONFIGURACIÓN
# =========================

DATASET_ID = "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr"
DATASET_PART = "latest"

BUOYS_FILE = "id_boyas.txt"
DOWNLOAD_DIR = Path("copernicus_boyas_tmp")
OUTPUT_JSON = "4_olas_boyas.json"

USERNAME = os.getenv("COPERNICUS_USERNAME", "magulles2")
PASSWORD = os.getenv("COPERNICUS_PASSWORD", "MiguelCMEMS2017")

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# UTILIDADES
# =========================

def safe_float(value):
    try:
        value = float(value)
        return None if np.isnan(value) else value
    except Exception:
        return None


def to_iso_time(t):
    try:
        return pd.to_datetime(t, utc=True).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def utc_now():
    return datetime.now(timezone.utc)


def yyyymmdd(dt):
    return dt.strftime("%Y%m%d")


def wanted_day():
    now = utc_now()
    today = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    return today - timedelta(days=1)


def parse_buoys_file(path):
    items = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if "|" in line:
                name, ident = line.split("|", 1)
                name = name.strip()
                ident = ident.strip()
            else:
                name = line
                ident = line

            items.append({
                "name": name,
                "identifier": ident
            })

    return items


# =========================
# DESCARGA (IR + GL)
# =========================

def build_filenames(identifier, day):
    date = yyyymmdd(day)
    return [
        f"IR_TS_MO_{identifier}_{date}.nc",
        f"GL_TS_MO_{identifier}_{date}.nc"
    ]


def download_file(filename):
    try:
        copernicusmarine.get(
            dataset_id=DATASET_ID,
            dataset_part=DATASET_PART,
            username=USERNAME,
            password=PASSWORD,
            filter=filename,
            output_directory=str(DOWNLOAD_DIR),
            no_directories=True,
            overwrite=True,
        )
    except Exception as e:
        print(f"[WARN] No se pudo descargar {filename}: {e}")
        return None

    local_path = DOWNLOAD_DIR / filename
    if local_path.exists():
        return local_path

    matches = list(DOWNLOAD_DIR.rglob(filename))
    return matches[0] if matches else None


# =========================
# INTERPOLACIÓN HORARIA
# =========================

def resample_to_hourly(times, values, target_day):
    try:
        times = pd.to_datetime(times, utc=True)
        s = pd.Series(values, index=times)

        start = pd.Timestamp(target_day)
        end = start + pd.Timedelta(hours=23)

        hourly_index = pd.date_range(start, end, freq="1H")

        s = s.reindex(s.index.union(hourly_index)).sort_index()
        s_interp = s.interpolate(method="time")

        s_hourly = s_interp.reindex(hourly_index)

        return hourly_index.to_pydatetime(), s_hourly.values

    except Exception as e:
        print(f"[WARN] interpolación falló: {e}")
        return times, values


# =========================
# HELPERS NETCDF
# =========================

def find_name(ds, candidates):
    for c in candidates:
        if c in ds.variables or c in ds.coords or c in ds.data_vars:
            return c
    return None


def extract_vhm0(da, time_name):
    arr = da

    if time_name in arr.dims:
        other_dims = [d for d in arr.dims if d != time_name]

        if len(other_dims) == 1:
            arr = arr.transpose(time_name, other_dims[0])
            vals = np.asarray(arr.values)

            if vals.ndim == 2:
                valid_counts = np.sum(np.isfinite(vals), axis=0)
                best_col = int(np.argmax(valid_counts))
                return vals[:, best_col]

        elif len(other_dims) == 0:
            return np.asarray(arr.values).reshape(-1)

    return np.asarray(arr.values).reshape(-1)


def extract_wspd(da, time_name):
    arr = da

    if time_name in arr.dims:
        other_dims = [d for d in arr.dims if d != time_name]

        if len(other_dims) == 1:
            arr = arr.transpose(time_name, other_dims[0])
            vals = np.asarray(arr.values)

            if vals.ndim == 2:
                valid_counts = np.sum(np.isfinite(vals), axis=0)
                best_col = int(np.argmax(valid_counts))
                return vals[:, best_col]

        elif len(other_dims) == 0:
            return np.asarray(arr.values).reshape(-1)

    return np.asarray(arr.values).reshape(-1)


# =========================
# CORE
# =========================

def extract_records(nc_path):
    with xr.open_dataset(nc_path) as ds:
        print(f"{nc_path.name} -> vars: {list(ds.data_vars)}")

        time_name = find_name(ds, ["TIME", "time", "Time"])
        lon_name = find_name(ds, ["LONGITUDE", "longitude", "lon", "LON"])
        lat_name = find_name(ds, ["LATITUDE", "latitude", "lat", "LAT"])
        hs_name = find_name(ds, ["VHM0", "vhm0"])
        wspd_name = find_name(ds, ["WSPD", "wspd"])

        if time_name is None:
            raise ValueError(f"No encuentro TIME en {nc_path.name}")

        times = np.asarray(ds[time_name].values).reshape(-1)

        lon = safe_float(ds[lon_name].values) if lon_name else None
        lat = safe_float(ds[lat_name].values) if lat_name else None

        hs_values = extract_vhm0(ds[hs_name], time_name) if hs_name else None
        wspd_values = extract_wspd(ds[wspd_name], time_name) if wspd_name else None

        # =========================
        # INTERPOLACIÓN
        # =========================
        if hs_values is not None and len(times) != 24:
            times, hs_values = resample_to_hourly(times, hs_values, wanted_day())

        if wspd_values is not None and len(times) != 24:
            _, wspd_values = resample_to_hourly(times, wspd_values, wanted_day())

        ntime = len(times)

        # =========================
        # BUILD RECORDS
        # =========================
        records = []

        for i in range(ntime):
            rec = {
                "time": to_iso_time(times[i]),
                "hsobs": safe_float(hs_values[i]) if hs_values is not None else None,
                "lon": lon,
                "lat": lat
            }

            if wspd_values is not None:
                rec["wspd"] = safe_float(wspd_values[i])

            records.append(rec)

        return records


# =========================
# MAIN
# =========================

def main():
    buoys = parse_buoys_file(BUOYS_FILE)
    day = wanted_day()

    result = {
        "source": "Copernicus Marine INSITU IBI NRT",
        "dataset_id": DATASET_ID,
        "generated_at": utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "target_day": yyyymmdd(day),
        "buoys": {}
    }

    for buoy in buoys:
        name = buoy["name"]
        ident = buoy["identifier"]

        filenames = build_filenames(ident, day)

        path = None
        for filename in filenames:
            path = download_file(filename)
            if path:
                print(f"[OK] descargado {filename}")
                break

        if path and path.exists():
            try:
                records = extract_records(path)
                result["buoys"][name] = records
            except Exception as e:
                print(f"[WARN] error leyendo {ident}: {e}")
                result["buoys"][name] = []
        else:
            print(f"[WARN] no encontrado {ident}")
            result["buoys"][name] = []

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    try:
        if DOWNLOAD_DIR.exists():
            shutil.rmtree(DOWNLOAD_DIR)
    except Exception as e:
        print(f"[WARN] no se pudo borrar {DOWNLOAD_DIR}: {e}")

    print(f"OK -> {OUTPUT_JSON}")


if __name__ == "__main__":
    main()