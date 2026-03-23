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

// Panel lateral
function updatePanel(site, hourIndex) {
  if (!infoPanel) return;

  const point = getForecastPoint(site, hourIndex);

  if (!point) {
    infoPanel.innerHTML = `<p><strong>Name:</strong> ${site.name}</p>`;
    return;
  }

  infoPanel.innerHTML = `
    <p><strong>Name:</strong> ${site.name}</p>
    <p><strong>Time:</strong> ${point.time || "--"}</p>
    <p><strong>Wave:</strong> ${formatNumber(point.wave)} m</p>
    <p><strong>Tp:</strong> ${formatNumber(point.tp)} s</p>
  `;
}

// 🔥 GRÁFICO CON DOBLE SERIE
function renderWaveChart(site, hourIndex) {
  if (!waveChartCanvas || typeof Chart === "undefined" || !site?.forecast?.length) return;

  const pdeSite = getPdeLocationByName(site.name);

  const labels = site.forecast.map(p => {
    if (!p.time) return "";
    const d = new Date(p.time);
    return `${d.getDate()} ${d.getHours()}h`;
  });

  const mainHs = site.forecast.map(p => p.wave);

  let pdeHs = [];
  if (pdeSite) {
    const mapPDE = new Map(pdeSite.forecast.map(p => [p.time, p.wave]));
    pdeHs = site.forecast.map(p => mapPDE.get(p.time) ?? null);
  }

  if (waveChart) waveChart.destroy();

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Hs Copernicus (m)",
          data: mainHs,
          borderColor: "#2563eb",
          fill: true,
          tension: 0.25
        },
        {
          label: "Hs Puertos (m)",
          data: pdeHs,
          borderColor: "#f97316",
          fill: false,
          tension: 0.25
        }
      ]
    }
  });
}

// Marcadores
function createMarkers() {
  locations.forEach(site => {
    const marker = L.circleMarker(site.coords, {
      radius: 6,
      color: "blue"
    }).addTo(map);

    marker.on("click", () => {
      selectedLocation = site;
      updatePanel(site, selectedHour);
      renderWaveChart(site, selectedHour);
    });
  });
}

// Carga principal
Promise.all([
  fetch("./meteo_points.json").then(r => r.json()),
  fetch("./meteo_points_pde.json").then(r => r.json())
])
.then(([meteoData, pdeData]) => {

  locations = meteoData.points.map(p => ({
    name: p.name,
    coords: [p.lat, p.lon],
    forecast: p.forecast.map(f => ({
      time: f.time,
      wave: f.hs,
      tp: f.tp
    }))
  }));

  pdeLocations = pdeData.points.map(p => ({
    name: p.name,
    forecast: p.forecast.map(f => ({
      time: f.time,
      wave: f.hs
    }))
  }));

  createMarkers();
});
