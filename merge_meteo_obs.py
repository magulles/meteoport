import json
from copy import deepcopy

METEO_FILE = "meteo_points.json"
OBS_FILE = "boyas_obs_1day.json"
OUTPUT_FILE = "meteo_points_tot.json"

with open(METEO_FILE, "r", encoding="utf-8") as f:
    meteo_data = json.load(f)

with open(OBS_FILE, "r", encoding="utf-8") as f:
    obs_data = json.load(f)

# --------------------------------------------------
# Construir diccionario de observaciones:
# obs_dict["valpalcomp1"]["2026-03-30T00:00:00Z"] = {
#     "hsobs": ...,
#     "wspdobs": ...
# }
# --------------------------------------------------
obs_dict = {}
buoys = obs_data.get("buoys", {})

for buoy_name, records in buoys.items():
    obs_dict[buoy_name] = {}

    for rec in records:
        t = rec.get("time")
        if t is None:
            continue

        obs_dict[buoy_name][t] = {
            "hsobs": rec.get("hsobs"),
            "wspdobs": rec.get("wspd")
        }

# --------------------------------------------------
# Trabajar sobre meteo_data["points"]
# --------------------------------------------------
points = meteo_data.get("points", [])

for point in points:
    point_name = point.get("name")
    forecast = point.get("forecast", [])

    obs_for_point = obs_dict.get(point_name, {})

    # Indexar forecast por tiempo
    forecast_dict = {}
    for row in forecast:
        t = row.get("time")
        if t is not None:
            forecast_dict[t] = deepcopy(row)

    # Añadir observaciones
    for t, obs_vals in obs_for_point.items():
        if t in forecast_dict:
            forecast_dict[t]["hsobs"] = obs_vals["hsobs"]
            forecast_dict[t]["wspdobs"] = obs_vals["wspdobs"]
        else:
            # Crear fila nueva con obs y resto null
            forecast_dict[t] = {
                "time": t,
                "hs": None,
                "tp": None,
                "di": None,
                "hs_pde": None,
                "tp_pde": None,
                "di_pde": None,
                "hs_port": None,
                "wind_speed_10m_ms": None,
                "wind_direction_10m_deg": None,
                "hsobs": obs_vals["hsobs"],
                "wspdobs": obs_vals["wspdobs"]
            }

    # Asegurar que todas las filas tengan hsobs y wspdobs
    for row in forecast_dict.values():
        if "hsobs" not in row:
            row["hsobs"] = None
        if "wspdobs" not in row:
            row["wspdobs"] = None

    # Ordenar por tiempo
    point["forecast"] = sorted(forecast_dict.values(), key=lambda x: x["time"])

# --------------------------------------------------
# Guardar resultado
# --------------------------------------------------
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(meteo_data, f, indent=2, ensure_ascii=False)

print(f"Archivo generado: {OUTPUT_FILE}")