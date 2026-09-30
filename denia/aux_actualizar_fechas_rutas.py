import json
from datetime import datetime, timezone

ROUTES_BASE_FILE = "routes_base.json"
ROUTES_OUTPUT_FILE = "routes.json"

def update_routes_dates(base_file=ROUTES_BASE_FILE, output_file=ROUTES_OUTPUT_FILE):
    today = datetime.now(timezone.utc).date()

    with open(base_file, "r", encoding="utf-8") as f:
        routes = json.load(f)

    for route in routes:
        dep_str = route.get("departure_time")
        arr_str = route.get("arrival_time")

        if not dep_str or not arr_str:
            continue

        dep_base = datetime.strptime(dep_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        arr_base = datetime.strptime(arr_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        duration = arr_base - dep_base

        new_departure = datetime.combine(today, dep_base.time(), tzinfo=timezone.utc)
        new_arrival = new_departure + duration

        route["departure_time"] = new_departure.strftime("%Y-%m-%dT%H:%M:%SZ")
        route["arrival_time"] = new_arrival.strftime("%Y-%m-%dT%H:%M:%SZ")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(routes, f, ensure_ascii=False, indent=2)

    print(f"{output_file} actualizado con la fecha de hoy conservando la duración real de cada ruta")


if __name__ == "__main__":
    update_routes_dates()