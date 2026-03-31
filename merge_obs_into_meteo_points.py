import json

METEO_FILE = "meteo_points.json"
OBS_FILE = "boyas_obs_1day.json"
OUTPUT_FILE = "meteo_points.json"


# =========================
# CARGA
# =========================

with open(METEO_FILE, "r", encoding="utf-8") as f:
    meteo = json.load(f)

with open(OBS_FILE, "r", encoding="utf-8") as f:
    obs = json.load(f)


# =========================
# CONVERTIR BOYAS
# =========================

new_points = []

for buoy_name, records in obs["buoys"].items():

    if not records:
        continue

    lon = records[0].get("lon")
    lat = records[0].get("lat")

    forecast = []

    for r in records:
        forecast.append({
            "time": r["time"],
            "hs": r.get("hsobs"),
            "tp": None,
            "di": None,
            "hs_pde": None,
            "tp_pde": None,
            "di_pde": None,
            "hs_port": None,
            "wind_speed_10m_ms": r.get("wspd"),
            "wind_direction_10m_deg": None
        })

    point = {
        "name": f"{buoy_name}_obs",
        "lon": lon,
        "lat": lat,
        "forecast": forecast
    }

    new_points.append(point)


# =========================
# INSERTAR EN METEO
# =========================

if "points" in meteo:
    meteo["points"].extend(new_points)
else:
    print("⚠️ No encuentro 'points'")


# =========================
# GUARDAR
# =========================

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(meteo, f, indent=2, ensure_ascii=False)


print("OK -> meteo_points actualizado con observaciones")
