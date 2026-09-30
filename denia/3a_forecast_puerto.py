# -*- coding: utf-8 -*-
"""Meteoport - forecast portuario DEMO.
Genera, para cada punto *_puerto de lonp_latp.txt, series horarias sintéticas
de agitación (hs_port_pred) y nivel del mar (sea_level_pred), desde -1 a +3 días.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json, math, random

POINTS_FILE = "lonp_latp.txt"
OUTPUT_JSON = "3a_forecast_puerto.json"
PAST_DAYS = 1
FUTURE_DAYS = 3
TOTAL_HOURS = (PAST_DAYS + FUTURE_DAYS) * 24

def read_points(filename):
    pts = []
    with open(filename, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"): continue
            p = line.split()
            if len(p) != 3:
                print(f"[AVISO] Línea {n} ignorada"); continue
            try: lon, lat = float(p[1]), float(p[2])
            except ValueError:
                print(f"[AVISO] Línea {n}: lon/lat no válidos"); continue
            pts.append({"point_id": len(pts)+1, "name": p[0], "lon": lon, "lat": lat})
    return pts

def synthetic_values(name, h):
    # Funciones deterministas compartidas conceptualmente con observaciones.
    seed = sum((i+1)*ord(c) for i, c in enumerate(name))
    phase = (seed % 360) * math.pi/180
    hs = 0.13 + 0.07*(1+math.sin(2*math.pi*h/38 + phase))/2 \
         + 0.025*math.sin(2*math.pi*h/17 + phase/2)
    # Marea semidiurna + componente más lenta de nivel medio/surge.
    sea = 0.42*math.sin(2*math.pi*h/12.42 + phase) \
          + 0.10*math.sin(2*math.pi*h/24 + phase/3)
    return max(0.01, hs), sea

def main():
    print("\n🌊 FORECAST PORTUARIO - DEMO\n")
    if not Path(POINTS_FILE).exists(): raise FileNotFoundError(POINTS_FILE)
    points = [p for p in read_points(POINTS_FILE) if "_puerto" in p["name"].lower()]
    if not points: raise ValueError("No hay puntos con '_puerto'.")

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=PAST_DAYS)
    out = []
    for p in points:
        rng = random.Random("forecast_" + p["name"])
        fc = []
        for i in range(TOTAL_HOURS + 1):
            t = start + timedelta(hours=i)
            h = (t.timestamp()/3600.0)
            hs, sea = synthetic_values(p["name"], h)
            # Error/modelo sintético pequeño y suave.
            hs += 0.008*math.sin(2*math.pi*i/29) + rng.uniform(-0.004, 0.004)
            sea += 0.025*math.sin(2*math.pi*i/31) + rng.uniform(-0.008, 0.008)
            fc.append({"time": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "hs_port_pred": round(max(0.01, hs), 3),
                       "sea_level_pred": round(sea, 3)})
        out.append({"point_id": p["point_id"], "name": p["name"],
                    "lon": p["lon"], "lat": p["lat"],
                    "source": "simulated_demo", "forecast": fc})
        print(f"{p['name']}: {len(fc)} registros")

    payload = {"summary":{"generated_at_utc":datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                          "past_days":PAST_DAYS, "forecast_days":FUTURE_DAYS,
                          "resolution":"1h", "source":"simulated_demo"},
               "points":out}
    with open(OUTPUT_JSON,"w",encoding="utf-8") as f:
        json.dump(payload,f,ensure_ascii=False,indent=2)
    print(f"\nOK: {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
