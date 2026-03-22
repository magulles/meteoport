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
let markers = [];

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

// Helpers
function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "--";
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

function updateHourLabel() {
  if (!hourLabel) return;

  const refPoint = locations.find(site => site.forecast && site.forecast[selectedHour]);
  if (refPoint) {
    hourLabel.textContent = refPoint.forecast[selectedHour].time || "--";
  } else {
    hourLabel.textContent = "--";
  }
}

// Popup
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
    <p><strong>Wave:</strong> ${formatNumber(point.wave)} m</p>
    <p><strong>Tp:</strong> ${formatNumber(point.tp)} s</p>
    <p><strong>Wave direction:</strong> ${formatNumber(point.dir)}°</p>
    <p><strong>Wind 10 m:</strong> ${formatNumber(point.windSpeed)} m/s</p>
    <p><strong>Wind direction:</strong> ${formatNumber(point.windDir)}°</p>
    <p><strong>Status:</strong> <span style="color:${color}; font-weight:600;">${label}</span></p>
    <p><strong>Thresholds:</strong> green ≤ ${site.thresholds.greenMax} m, orange ≤ ${site.thresholds.orangeMax} m</p>
  `;
}

// Gráfico
function renderWaveChart(site, hourIndex) {
  if (!waveChartCanvas || typeof Chart === "undefined" || !site?.forecast?.length) return;

  const labels = site.forecast.map(p => p.time || p.hour);
  const values = site.forecast.map(p => p.wave);

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
      datasets: [{
        label: "Hs (m)",
        data: values,
        borderColor: "#2563eb",
        backgroundColor: "rgba(37, 99, 235, 0.12)",
        fill: true,
        tension: 0.25,
        pointBackgroundColor: pointColors,
        pointRadius: pointRadii
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false
    }
  });
}

// Marcadores
function createMarkers() {
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

// Cargar JSON NUEVO
fetch("./meteo_points.json")
  .then(res => res.json())
  .then(data => {

    const rawPoints = Array.isArray(data) ? data : data.points;

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

    hourSlider.max = getForecastLength() - 1;
    createMarkers();
    updateHourLabel();
  });

// Slider
hourSlider.addEventListener("input", e => {
  selectedHour = Number(e.target.value);
  refreshMarker();
});
