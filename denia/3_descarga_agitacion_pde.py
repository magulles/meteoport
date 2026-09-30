# -*- coding: utf-8 -*-
"""Meteoport: agitación portuaria DEMO.
Procesa puntos de lonp_latp.txt cuyo nombre contenga "_puerto" y genera
1 día pasado + 3 días futuros de hs_port horaria simulada.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json, math, random

POINTS_FILE = "lonp_latp.txt"
PORT_OUTPUT_JSON = "3_agitacion.json"
PAST_DAYS = 1
PORT_FORECAST_DAYS = 3
PORT_TOTAL_HOURS = (PAST_DAYS + PORT_FORECAST_DAYS) * 24

def read_points(filename):
    points = []
    with open(filename, "r", encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) != 3:
                print(f"[AVISO] Línea {n} ignorada")
                continue
            name, lon, lat = parts
            try:
                lon, lat = float(lon), float(lat)
            except ValueError:
                print(f"[AVISO] Línea {n}: lon/lat no válidos")
                continue
            points.append({"point_id": len(points)+1, "name": name,
                           "lon": lon, "lat": lat})
    return points

def generate_demo(point):
    # Semilla estable por nombre: cada punto tiene una señal propia.
    rng = random.Random(point["name"])
    now = datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = today - timedelta(days=PAST_DAYS)

    base = rng.uniform(0.08, 0.16)
    a1 = rng.uniform(0.04, 0.10)
    a2 = rng.uniform(0.01, 0.04)
    p1 = rng.uniform(0, 2*math.pi)
    p2 = rng.uniform(0, 2*math.pi)

    forecast, previous = [], None
    for h in range(PORT_TOTAL_HOURS):
        hs = (base
              + a1*(1 + math.sin(2*math.pi*h/36 + p1))/2
              + a2*math.sin(2*math.pi*h/15 + p2)
              + rng.uniform(-0.012, 0.012))
        if previous is not None:
            hs = 0.72*previous + 0.28*hs
        hs = max(0.01, hs)
        previous = hs
        forecast.append({
            "time": (start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "hs_port": round(hs, 2)
        })
    return forecast

def main():
    print("\n🌊 PIPELINE AGITACIÓN PORTUARIA - DEMO\n")
    if not Path(POINTS_FILE).exists():
        raise FileNotFoundError(f"No existe {POINTS_FILE}")

    all_points = read_points(POINTS_FILE)
    points = [p for p in all_points if "_puerto" in p["name"].lower()]
    print(f"Se han leído {len(all_points)} puntos")
    print(f"Puntos con '_puerto': {len(points)}")
    if not points:
        raise ValueError("No hay puntos con '_puerto' en el archivo de entrada.")

    out_points = []
    for p in points:
        fc = generate_demo(p)
        print(f"Generando DEMO: {p['name']} | {len(fc)} horas")
        out_points.append({
            "point_id": p["point_id"],
            "name": p["name"],
            "requested_lon": p["lon"],
            "requested_lat": p["lat"],
            "lon": p["lon"],
            "lat": p["lat"],
            "forecast": fc,
            "source": "simulated_demo",
            "port_search_info": {
                "mesh_key": None,
                "valid_count": len(fc),
                "total_count": len(fc),
                "valid_ratio": 1.0,
                "simulated": True
            }
        })

    output = {
        "summary": {
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "past_days": PAST_DAYS,
            "agitacion_forecast_days": PORT_FORECAST_DAYS,
            "agitacion_forecast_hours": PORT_TOTAL_HOURS,
            "points_total": len(out_points),
            "total_agitacion_records": sum(len(p["forecast"]) for p in out_points),
            "source": "simulated_demo"
        },
        "points": out_points
    }

    with open(PORT_OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nOK. Archivo final guardado en: {PORT_OUTPUT_JSON}")
    print("Proceso completado correctamente.")

if __name__ == "__main__":
    main()



