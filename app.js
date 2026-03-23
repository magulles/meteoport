const FORECAST_HOURS = 72;

// Umbrales globales
const THRESHOLDS = {
  greenMax: 1.0,
  orangeMax: 2.0
};

// Estado global
let selectedHour = 0;
let selectedLocation = null;
let waveChart = null;
let locations = [];
let pdeLocations = [];
let markers = [];
let routes = [];
let routeLines = [];

// Referencias a elementos del DOM
const infoPanel = document.getElementById("info-panel");
const hourSlider = document.getElementById("hour-slider");
const hourLabel = document.getElementById("hour-label");
const waveChartCanvas = document.getElementById("wave-chart");

// Inicialización del mapa
const map = L.map("map").setView([39.5, 0], 5);

// Basemap
L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  attribution: "&copy; OpenStreetMap &copy; CARTO",
  subdomains: "abcd",
  maxZoom: 19
}).addTo(map);

// Forzar recálculo de tamaño por si el contenedor tarda en asentarse
setTimeout(() => {
  map.invalidateSize();
}, 200);

// Helpers
function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value) || value === -Infinity) return "--";
  return Number(value).toFixed(digits);
}

function getForecastLength() {
  if (!locations.length) return 0;
  return Math.max(...locations.map(site => (site.forecast ? site.forecast.length : 0)));
}

function getStatusColor(wave, thresholds) {
  if (wave === null || wave === undefined || Number.isNaN(wave)) return "gray";
  if (wave <= thresholds.greenMax) return "green";
  if (wave <= thresholds.orangeMax) return "orange";
  return "red";
}

function getStatusLabel(color) {
  if (color === "green") return "Operational";
  if (color === "orange") return "Caution";
  if (color === "gray") return "No data";
  return "High risk";
}

function getForecastPoint(site, hourIndex) {
  if (!site || !site.forecast || !site.forecast.length) return null;
  return site.forecast[hourIndex] || null;
}

function getLocationByName(name) {
  return locations.find(loc => loc.name === name);
}

function getPdeLocationByName(name) {
  return pdeLocations.find(loc => loc.name === name);
}

function updateHourLabel() {
  if (!hourLabel) return;

  const refPoint = locations.find(site => site.forecast && site.forecast[selectedHour]);
  if (refPoint) {
    hourLabel.textContent = refPoint.forecast[selectedHour].time || "--";
  } else {
    hourLabel.textContent = "--";
  }
}

// Busca el forecast más cercano a una fecha dada
function findClosestForecast(site, targetDate) {
  if (!site || !site.forecast || !site.forecast.length) return null;

  let best = null;
  let bestDiff = Infinity;

  site.forecast.forEach(f => {
    if (!f.time) return;

    const diff = Math.abs(new Date(f.time) - targetDate);
    if (diff < bestDiff) {
      bestDiff = diff;
      best = f;
    }
  });

  if (bestDiff <= 61 * 60 * 1000) return best;
  return null;
}

// Popup puntos
function getPopupContent(site, hourIndex) {
  const point = getForecastPoint(site, hourIndex);

  if (!point) {
    return `<strong>${site.name}</strong><br>No data for this hour`;
  }

  const color = getStatusColor(point.wave, site.thresholds);
  const label = getStatusLabel(color);

  return `
    <strong>${site.name}</strong><br>
    Time: ${point.time || "--"}<br>
    Wave: ${formatNumber(point.wave)} m<br>
    Tp: ${formatNumber(point.tp)} s<br>
    Wave dir: ${formatNumber(point.dir)}°<br>
    Wind 10 m: ${formatNumber(point.windSpeed)} m/s<br>
    Wind dir: ${formatNumber(point.windDir)}°<br>
    Status: <span style="color:${color}; font-weight:600;">${label}</span>
  `;
}

// Panel lateral
function updatePanel(site, hourIndex) {
  if (!infoPanel) return;

  const point = getForecastPoint(site, hourIndex);
  const pdeSite = getPdeLocationByName(site.name);
  const pdePoint = getForecastPoint(pdeSite, hourIndex);

  if (!point) {
    infoPanel.innerHTML = `
      <p><strong>Name:</strong> ${site.name}</p>
      <p><strong>Status:</strong> No data for this hour</p>
    `;
    return;
  }

  const color = getStatusColor(point.wave, site.thresholds);
  const label = getStatusLabel(color);

  infoPanel.innerHTML = `
    <p><strong>Name:</strong> ${site.name}</p>
    <p><strong>Time:</strong> ${point.time || "--"}</p>
    <p><strong>Wave (Copernicus):</strong> ${formatNumber(point.wave)} m</p>
    <p><strong>Wave (Puertos):</strong> ${formatNumber(pdePoint?.wave)} m</p>
    <p><strong>Tp:</strong> ${formatNumber(point.tp)} s</p>
    <p><strong>Wave direction:</strong> ${formatNumber(point.dir)}°</p>
    <p><strong>Wind 10 m:</strong> ${formatNumber(point.windSpeed)} m/s</p>
    <p><strong>Wind direction:</strong> ${formatNumber(point.windDir)}°</p>
    <p><strong>Status:</strong> <span style="color:${color}; font-weight:600;">${label}</span></p>
    <p><strong>Thresholds:</strong> green ≤ ${site.thresholds.greenMax} m, orange ≤ ${site.thresholds.orangeMax} m</p>
  `;
}

function formatChartLabel(time) {
  if (!time) return "";

  const date = new Date(time);
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");

  const months = ["ene", "feb", "mar", "abr", "may", "jun",
                  "jul", "ago", "sep", "oct", "nov", "dic"];

  const monthName = months[date.getMonth()];
  return `${day} ${monthName} ${hours}h`;
}

// Gráfico
function renderWaveChart(site, hourIndex) {
  if (!waveChartCanvas || typeof Chart === "undefined" || !site?.forecast?.length) return;

  const pdeSite = getPdeLocationByName(site.name);

  const labels = site.forecast.map(p => formatChartLabel(p.time));
  const mainValues = site.forecast.map(p => p.wave);

  let pdeValues = [];
  if (pdeSite?.forecast?.length) {
    const pdeMap = new Map(
      pdeSite.forecast.map(p => [p.time, p.wave])
    );
    pdeValues = site.forecast.map(p => pdeMap.get(p.time) ?? null);
  } else {
    pdeValues = site.forecast.map(() => null);
  }

  const pointColors = site.forecast.map((p, i) =>
    i === hourIndex ? "red" : "#2563eb"
  );

  const pointRadii = site.forecast.map((p, i) =>
    i === hourIndex ? 5 : 2
  );

  if (waveChart) waveChart.destroy();

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Hs Copernicus (m)",
          data: mainValues,
          borderColor: "#2563eb",
          backgroundColor: "rgba(37, 99, 235, 0.12)",
          fill: true,
          tension: 0.25,
          pointBackgroundColor: pointColors,
          pointRadius: pointRadii
        },
        {
          label: "Hs Puertos (m)",
          data: pdeValues,
          borderColor: "#f97316",
          backgroundColor: "rgba(249, 115, 22, 0.08)",
          fill: false,
          tension: 0.25,
          pointRadius: 0,
          spanGaps: true
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false
      }
    }
  });
}

// Marcadores
function createMarkers() {
  markers.forEach(({ marker }) => map.removeLayer(marker));
  markers = [];

  locations.forEach(site => {
    const point = getForecastPoint(site, selectedHour);
    const color = point ? getStatusColor(point.wave, site.thresholds) : "gray";

    const marker = L.circleMarker(site.coords, {
      radius: 6,
      color,
      fillColor: color,
      fillOpacity: 0.8,
      weight: 2
    }).addTo(map);

    marker.bindPopup(getPopupContent(site, selectedHour));

    marker.on("click", () => {
      selectedLocation = site;
      updatePanel(site, selectedHour);
      renderWaveChart(site, selectedHour);
    });

    markers.push({ marker, site });
  });

  if (locations.length > 0) {
    const bounds = L.latLngBounds(locations.map(site => site.coords));
    map.fitBounds(bounds, { padding: [30, 30] });
  }
}

// Refrescar
function refreshMarker() {
  markers.forEach(({ marker, site }) => {
    const point = getForecastPoint(site, selectedHour);
    const color = point ? getStatusColor(point.wave, site.thresholds) : "gray";

    marker.setStyle({ color, fillColor: color });
    marker.setPopupContent(getPopupContent(site, selectedHour));
  });

  updateHourLabel();

  if (selectedLocation) {
    updatePanel(selectedLocation, selectedHour);
    renderWaveChart(selectedLocation, selectedHour);
  }
}

// Evalúa ruta
function evaluateRoute(route) {
  const start = new Date(route.departure_time);
  const end = new Date(route.arrival_time);

  const numPoints = route.points.length;
  if (numPoints === 0) {
    return null;
  }

  const totalMs = end - start;
  const stepMs = numPoints > 1 ? totalMs / (numPoints - 1) : 0;

  let hsMax = -Infinity;
  let windMax = -Infinity;
  let tpMax = -Infinity;

  let hsInfo = null;
  let windInfo = null;
  let tpInfo = null;

  const sampled = [];

  route.points.forEach((pointName, i) => {
    const site = getLocationByName(pointName);
    if (!site) return;

    const targetTime = new Date(start.getTime() + i * stepMs);
    const f = findClosestForecast(site, targetTime);

    sampled.push({
      pointName,
      targetTime: targetTime.toISOString(),
      forecast: f
    });

    if (!f) return;

    if (f.wave !== null && f.wave !== undefined && !Number.isNaN(f.wave) && f.wave > hsMax) {
      hsMax = f.wave;
      hsInfo = { point: pointName, time: f.time };
    }

    if (f.windSpeed !== null && f.windSpeed !== undefined && !Number.isNaN(f.windSpeed) && f.windSpeed > windMax) {
      windMax = f.windSpeed;
      windInfo = { point: pointName, time: f.time };
    }

    if (f.tp !== null && f.tp !== undefined && !Number.isNaN(f.tp) && f.tp > tpMax) {
      tpMax = f.tp;
      tpInfo = { point: pointName, time: f.time };
    }
  });

  return {
    hsMax,
    windMax,
    tpMax,
    hsInfo,
    windInfo,
    tpInfo,
    sampled
  };
}

function buildRoutePopup(route, result) {
  if (!result) {
    return `
      <strong>${route.name}</strong><br>
      No route data
    `;
  }

  return `
    <strong>${route.name}</strong><br>
    Departure: ${route.departure_time}<br>
    Arrival: ${route.arrival_time}<br>
    Points: ${route.points.join(" → ")}<br><br>

    <strong>Hs max:</strong> ${formatNumber(result.hsMax)} m<br>
    at ${result.hsInfo?.point || "--"} (${result.hsInfo?.time || "--"})<br><br>

    <strong>Wind max:</strong> ${formatNumber(result.windMax)} m/s<br>
    at ${result.windInfo?.point || "--"} (${result.windInfo?.time || "--"})<br><br>

    <strong>Tp max:</strong> ${formatNumber(result.tpMax)} s<br>
    at ${result.tpInfo?.point || "--"} (${result.tpInfo?.time || "--"})
  `;
}

// Dibuja rutas
function drawRoutes() {
  routeLines.forEach(line => map.removeLayer(line));
  routeLines = [];

  routes.forEach(route => {
    const coords = route.points
      .map(name => {
        const loc = getLocationByName(name);
        return loc ? loc.coords : null;
      })
      .filter(c => c !== null);

    if (coords.length < 2) return;

    const line = L.polyline(coords, {
      color: "red",
      weight: 3,
      opacity: 0.85
    }).addTo(map);

    line.on("click", () => {
      const result = evaluateRoute(route);
      line.bindPopup(buildRoutePopup(route, result)).openPopup();
    });

    routeLines.push(line);
  });
}

// Carga principal
Promise.all([
  fetch("./meteo_points.json").then(res => {
    if (!res.ok) throw new Error(`HTTP error meteo_points.json: ${res.status}`);
    return res.json();
  }),
  fetch("./meteo_points_pde.json").then(res => {
    if (!res.ok) throw new Error(`HTTP error meteo_points_pde.json: ${res.status}`);
    return res.json();
  }),
  fetch("./routes.json").then(res => {
    if (!res.ok) throw new Error(`HTTP error routes.json: ${res.status}`);
    return res.json();
  })
])
  .then(([meteoData, pdeData, routesData]) => {
    const rawPoints = Array.isArray(meteoData) ? meteoData : meteoData.points;
    const rawPdePoints = Array.isArray(pdeData) ? pdeData : pdeData.points;

    locations = rawPoints.map(point => ({
      name: point.name,
      coords: [point.lat, point.lon],
      thresholds: { ...THRESHOLDS },
      forecast: (point.forecast || []).map((f, i) => ({
        hour: i,
        time: f.time,
        wave: f.hs,
        tp: f.tp,
        dir: f.di,
        windSpeed: f.wind_speed_10m_ms,
        windDir: f.wind_direction_10m_deg
      }))
    }));

    pdeLocations = rawPdePoints.map(point => ({
      name: point.name,
      coords: [point.lat, point.lon],
      forecast: (point.forecast || []).map((f, i) => ({
        hour: i,
        time: f.time,
        wave: f.hs,
        tp: f.tp,
        dir: f.di
      }))
    }));

    routes = routesData;

    if (hourSlider) {
      const maxForecastLength = Math.max(getForecastLength() - 1, 0);
      hourSlider.max = maxForecastLength;
      hourSlider.value = selectedHour;
    }

    createMarkers();
    drawRoutes();
    updateHourLabel();

    if (locations.length) {
      selectedLocation = locations[0];
      updatePanel(selectedLocation, selectedHour);
      renderWaveChart(selectedLocation, selectedHour);
    }

    setTimeout(() => {
      map.invalidateSize();
    }, 300);
  })
  .catch(error => {
    console.error("Error cargando datos:", error);
    if (infoPanel) {
      infoPanel.innerHTML = `<p><strong>Error:</strong> ${error.message}</p>`;
    }
  });

// Slider
if (hourSlider) {
  hourSlider.addEventListener("input", e => {
    selectedHour = Number(e.target.value);
    refreshMarker();
  });
}
     
