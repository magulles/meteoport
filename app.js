// ============================
// CONFIG
// ============================

const THRESHOLDS = {
  greenMax: 1.0,
  orangeMax: 2.0
};

let selectedHour = 0;
let selectedLocation = null;
let selectedRoute = null;
let waveChart = null;

let locations = [];
let markers = [];
let routes = [];
let routeLayers = [];

// DOM
const infoPanel = document.getElementById("info-panel");
const hourSlider = document.getElementById("hour-slider");
const hourLabel = document.getElementById("hour-label");
const waveChartCanvas = document.getElementById("wave-chart");

// ============================
// MAPA
// ============================

const map = L.map("map").setView([39.5, 0], 5);

L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  attribution: "&copy; OpenStreetMap &copy; CARTO",
  subdomains: "abcd",
  maxZoom: 19
}).addTo(map);

// ============================
// HELPERS
// ============================

function formatNumber(val, decimals = 2) {
  if (val === null || val === undefined || Number.isNaN(val)) return "-";
  return Number(val).toFixed(decimals);
}

function formatTimeLabel(isoTime) {
  if (!isoTime) return "--";

  const d = new Date(isoTime);
  const months = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"];

  const month = months[d.getUTCMonth()];
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");

  return `${month}-${day}-${hour}h`;
}

function getColor(hs) {
  if (hs === null || hs === undefined || Number.isNaN(hs)) return "#9ca3af";
  if (hs <= THRESHOLDS.greenMax) return "green";
  if (hs <= THRESHOLDS.orangeMax) return "orange";
  return "red";
}

function getHexColorFromHs(hs) {
  if (hs === null || hs === undefined || Number.isNaN(hs)) return "#94a3b8";
  if (hs <= THRESHOLDS.greenMax) return "#16a34a";
  if (hs <= THRESHOLDS.orangeMax) return "#f59e0b";
  return "#dc2626";
}

function getOperationalWave(f) {
  const hasPde = f && f.hs_pde !== null && f.hs_pde !== undefined && !Number.isNaN(f.hs_pde);

  if (hasPde) {
    return {
      wave: f.hs_pde,
      tp: f.tp_pde,
      dir: f.di_pde,
      source: "PdE"
    };
  }

  return {
    wave: f?.hs ?? null,
    tp: f?.tp ?? null,
    dir: f?.di ?? null,
    source: "Copernicus"
  };
}

function buildMergedForecast(point) {
  return (point.forecast || []).map((f, i) => {
    const op = getOperationalWave(f);

    return {
      hour: i,
      time: f.time,
      wave: op.wave,
      tp: op.tp,
      dir: op.dir,
      waveSource: op.source,
      wavePde: f.hs_pde ?? null,
      waveCopernicus: f.hs ?? null,
      tpPde: f.tp_pde ?? null,
      tpCopernicus: f.tp ?? null,
      dirPde: f.di_pde ?? null,
      dirCopernicus: f.di ?? null,
      windSpeed: f.wind_speed_10m_ms ?? null,
      windDir: f.wind_direction_10m_deg ?? null
    };
  });
}

function getForecastLength() {
  if (!locations.length) return 0;
  return locations[0].forecast.length;
}

function findLocationByName(name) {
  return locations.find(loc => loc.name === name) || null;
}

// ============================
// CARGA DATOS
// ============================

Promise.all([
  fetch("./meteo_points.json").then(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status} cargando meteo_points.json`);
    return res.json();
  }),
  fetch("./routes.json").then(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status} cargando routes.json`);
    return res.json();
  })
])
  .then(([meteoData, routesData]) => {
    const rawPoints = Array.isArray(meteoData) ? meteoData : meteoData.points;

    locations = rawPoints.map(point => ({
      name: point.name,
      coords: [point.lat, point.lon],
      thresholds: { ...THRESHOLDS },
      forecast: buildMergedForecast(point)
    }));

    routes = routesData;

    const maxHour = Math.max(0, getForecastLength() - 1);
    hourSlider.max = maxHour;
    hourSlider.value = selectedHour;

    initMarkers();
    updateHourLabel();
  });

// ============================
// MARKERS
// ============================

function initMarkers() {
  markers.forEach(({ marker }) => map.removeLayer(marker));
  markers = [];

  locations.forEach(loc => {
    const f = loc.forecast[selectedHour];
    const color = f ? getColor(f.wave) : "#9ca3af";

    const marker = L.circleMarker(loc.coords, {
      radius: 4,
      color,
      fillColor: color,
      fillOpacity: 0.85,
      weight: 2
    }).addTo(map);

    marker.on("click", () => {
      selectedLocation = loc;
      updateInfoPanel();
      renderChart();
    });

    markers.push({ marker, loc });
  });
}

function updateMarkers() {
  markers.forEach(({ marker, loc }) => {
    const f = loc.forecast[selectedHour];
    const color = f ? getColor(f.wave) : "#9ca3af";

    marker.setStyle({ color, fillColor: color });
  });
}

// ============================
// PANEL
// ============================

function updateInfoPanel() {
  if (!selectedLocation) return;

  const f = selectedLocation.forecast[selectedHour];

  infoPanel.innerHTML = `
    <h3>${selectedLocation.name}</h3>
    <p>Hs: ${formatNumber(f.wave)} m</p>
    <p>Tp: ${formatNumber(f.tp)} s</p>
    <p>Dir: ${formatNumber(f.dir)}°</p>
  `;
}

// ============================
// GRÁFICA
// ============================

function renderChart() {
  if (!selectedLocation) return;

  const forecast = selectedLocation.forecast;
  const labels = forecast.map(f => formatTimeLabel(f.time));
  const hsPde = forecast.map(f => f.wavePde);
  const hsCop = forecast.map(f => f.waveCopernicus);

  if (waveChart) waveChart.destroy();

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "PdE",
          data: hsPde
        },
        {
          label: "Copernicus",
          data: hsCop
        }
      ]
    }
  });
}

// ============================
// SLIDER
// ============================

hourSlider.addEventListener("input", e => {
  selectedHour = parseInt(e.target.value, 10);
  updateMarkers();
  updateInfoPanel();
  updateHourLabel();
});

function updateHourLabel() {
  if (!locations.length) {
    hourLabel.innerText = "--";
    return;
  }

  const f = locations[0]?.forecast?.[selectedHour];
  hourLabel.innerText = f?.time ? formatTimeLabel(f.time) : "--";
}

