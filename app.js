// ============================
// CONFIG
// ============================

const FORECAST_HOURS = 120;

const THRESHOLDS = {
  greenMax: 1.0,
  orangeMax: 2.0
};

let selectedHour = 0;
let selectedLocation = null;
let waveChart = null;
let locations = [];
let markers = [];

// DOM
const infoPanel = document.getElementById("info-panel");
const hourSlider = document.getElementById("hour-slider");
const hourLabel = document.getElementById("hour-label");
const waveChartCanvas = document.getElementById("wave-chart");

// ============================
// MAPA (igual que antes)
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
  if (val === null || val === undefined || isNaN(val)) return "-";
  return Number(val).toFixed(decimals);
}

// 👉 PDE → Copernicus
function getOperationalWave(f) {
  const hasPde = f && f.hs_pde !== null && f.hs_pde !== undefined && !Number.isNaN(f.hs_pde);

  if (hasPde) {
    return {
      wave: f.hs_pde,
      tp: f.tp_pde,
      dir: f.di_pde,
      source: "PDE"
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
      waveCopernicus: f.hs ?? null,
      wavePde: f.hs_pde ?? null,
      tpPde: f.tp_pde ?? null,
      dirPde: f.di_pde ?? null,
      windSpeed: f.wind_speed_10m_ms ?? null,
      windDir: f.wind_direction_10m_deg ?? null
    };
  });
}

function getColor(hs) {
  if (hs <= THRESHOLDS.greenMax) return "green";
  if (hs <= THRESHOLDS.orangeMax) return "orange";
  return "red";
}

// ============================
// CARGA DATOS
// ============================

fetch("./meteo_points.json")
  .then(res => res.json())
  .then(data => {
    locations = data.points.map(point => ({
      name: point.name,
      coords: [point.lat, point.lon],
      thresholds: { ...THRESHOLDS },
      forecast: buildMergedForecast(point)
    }));

    initMarkers();

    hourSlider.max = FORECAST_HOURS - 1;
    updateHourLabel();
  });

// ============================
// MARKERS
// ============================

function initMarkers() {
  locations.forEach(loc => {
    const f = loc.forecast[selectedHour];

    const marker = L.circleMarker(loc.coords, {
      radius: 6,
      color: getColor(f.wave),
      fillOpacity: 0.8
    }).addTo(map);

    marker.on("click", () => {
      selectedLocation = loc;
      updateInfoPanel();
      updateChart();
    });

    markers.push({ marker, loc });
  });
}

function updateMarkers() {
  markers.forEach(({ marker, loc }) => {
    const f = loc.forecast[selectedHour];

    marker.setStyle({
      color: getColor(f.wave)
    });
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
    <p><b>Hora:</b> ${f.time}</p>
    <p><b>Hs:</b> ${formatNumber(f.wave)} m (${f.waveSource})</p>
    <p><b>Tp:</b> ${formatNumber(f.tp)} s</p>
    <p><b>Dir:</b> ${formatNumber(f.dir)}°</p>
    <p><b>Viento:</b> ${formatNumber(f.windSpeed)} m/s</p>
    <p><b>Dir viento:</b> ${formatNumber(f.windDir)}°</p>
  `;
}

// ============================
// GRÁFICA (ARREGLADA)
// ============================

function updateChart() {
  if (!selectedLocation) return;

  const labels = selectedLocation.forecast.map(f => {
    const d = new Date(f.time);
    return `${d.getDate()}-${d.getHours()}h`;
  });

  const main = selectedLocation.forecast.map(f => f.wave);
  const pde = selectedLocation.forecast.map(f => f.wavePde);
  const cop = selectedLocation.forecast.map(f => f.waveCopernicus);

  const container = waveChartCanvas.parentElement;
  if (container) container.style.height = "260px";

  waveChartCanvas.height = 260;

  if (waveChart) waveChart.destroy();

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Principal (PDE→Copernicus)",
          data: main,
          borderWidth: 2,
          tension: 0.3
        },
        {
          label: "PDE",
          data: pde,
          borderDash: [5, 5]
        },
        {
          label: "Copernicus",
          data: cop,
          borderDash: [2, 2]
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false
    }
  });
}

// ============================
// SLIDER
// ============================

hourSlider.addEventListener("input", e => {
  selectedHour = parseInt(e.target.value);
  updateMarkers();
  updateInfoPanel();
  updateChart();
  updateHourLabel();
});

function updateHourLabel() {
  hourLabel.innerText = `+${selectedHour}h`;
}
