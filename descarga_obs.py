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

DATASET_ID = "cmems_obs-ins_ibi_phybgcwav_mynrt_na_irr"
DATASET_PART = "latest"

BUOYS_FILE = "name_boyas.txt"
DOWNLOAD_DIR = Path("copernicus_boyas_tmp")
OUTPUT_JSON = "boyas_obs_1day.json"

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


def build_filename(identifier, day):
    return f"IR_TS_MO_{identifier}_{yyyymmdd(day)}.nc"


# =========================
# HELPERS NETCDF
# =========================

def find_name(ds, candidates):
    for c in candidates:
        if c in ds.variables or c in ds.coords or c in ds.data_vars:
            return c
    return None


def find_depth_dim(da):
    for d in ["DEPTH", "DEPH", "depth", "Depth"]:
        if d in da.dims:
            return d
    return None


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
# EXTRACCIÓN VARIABLES
# =========================

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


def compute_hs_from_spectrum(ds, spec_name, time_name):
    spec = ds[spec_name]

    freq_name = find_name(
        ds,
        ["FREQUENCY", "frequency", "freq", "FREQ", "wave_frequency", "spectral_frequency"]
    )
    if freq_name is None:
        raise ValueError(f"No encuentro eje de frecuencia para {spec_name}")

    freq = np.asarray(ds[freq_name].values).reshape(-1)

    other_dims = [d for d in spec.dims if d != time_name]
    if not other_dims:
        raise ValueError(f"{spec_name} no tiene dimensión de frecuencia")

    freq_dim = None
    for d in other_dims:
        if spec.sizes[d] == len(freq):
            freq_dim = d
            break

    if freq_dim is None:
        raise ValueError(
            f"No pude identificar dimensión de frecuencia en {spec_name}. "
            f"dims={spec.dims}, len(freq)={len(freq)}"
        )

    spec = spec.transpose(time_name, freq_dim)
    arr = np.asarray(spec.values)

    m0 = np.trapz(arr, x=freq, axis=1)
    hs = 4.0 * np.sqrt(np.maximum(m0, 0.0))
    return np.asarray(hs).reshape(-1)


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
        spec_name = find_name(ds, ["VSPEC1D", "vspec1d"])
        wspd_name = find_name(ds, ["WSPD", "wspd"])

        if time_name is None:
            raise ValueError(f"No encuentro TIME en {nc_path.name}")

        times = np.asarray(ds[time_name].values).reshape(-1)
        ntime = len(times)

        lon = safe_float(ds[lon_name].values) if lon_name else None
        lat = safe_float(ds[lat_name].values) if lat_name else None

        # HS: prioridad VHM0 -> VSPEC1D -> None
        if hs_name is not None:
            hs_values = extract_vhm0(ds[hs_name], time_name)
        elif spec_name is not None:
            hs_values = compute_hs_from_spectrum(ds, spec_name, time_name)
        else:
            hs_values = None

        # WSPD opcional
        if wspd_name is not None:
            wspd_values = extract_wspd(ds[wspd_name], time_name)
        else:
            wspd_values = None

        records = []
        for i in range(ntime):
            rec = {
                "time": to_iso_time(times[i]),
                "hsobs": safe_float(hs_values[i]) if hs_values is not None and i < len(hs_values) else None,
                "lon": lon,
                "lat": lat
            }

            if wspd_values is not None and i < len(wspd_values):
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

        filename = build_filename(ident, day)
        path = download_file(filename)

        if path and path.exists():
            try:
                records = extract_records(path)
                result["buoys"][name] = records
            except Exception as e:
                print(f"[WARN] error leyendo {filename}: {e}")
                result["buoys"][name] = []
        else:
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
