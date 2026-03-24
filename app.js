const FORECAST_HOURS = 120;

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

// DOM
const infoPanel = document.getElementById("info-panel");
const hourSlider = document.getElementById("hour-slider");
const hourLabel = document.getElementById("hour-label");
const waveChartCanvas = document.getElementById("wave-chart");

// ============================
// MAPA
// ============================

const map = L.map("map").setView([39.5, -0.5], 5);

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "© OpenStreetMap"
}).addTo(map);

// ============================
// CARGA JSON
// ============================

fetch("meteo_points.json")
  .then(res => res.json())
  .then(data => {
    locations = data.points;
    initMarkers();
  });

// ============================
// HELPERS
// ============================

// 👉 OLEAJE PRINCIPAL (PDE → Copernicus)
function getWaveData(forecast) {
  if (forecast.hs_pde !== null && forecast.hs_pde !== undefined) {
    return {
      hs: forecast.hs_pde,
      tp: forecast.tp_pde,
      di: forecast.di_pde,
      source: "PDE"
    };
  }

  return {
    hs: forecast.hs,
    tp: forecast.tp,
    di: forecast.di,
    source: "Copernicus"
  };
}

// 👉 VIENTO (Open-Meteo por ahora)
function getWindData(forecast) {
  return {
    speed: forecast.wind_speed_10m_ms,
    direction: forecast.wind_direction_10m_deg
  };
}

// 👉 COLOR POR ALTURA DE OLA
function getColor(hs) {
  if (hs <= THRESHOLDS.greenMax) return "green";
  if (hs <= THRESHOLDS.orangeMax) return "orange";
  return "red";
}

// ============================
// MARKERS
// ============================

function initMarkers() {
  locations.forEach(loc => {
    const forecast = loc.forecast[selectedHour];
    const wave = getWaveData(forecast);

    const marker = L.circleMarker([loc.lat, loc.lon], {
      radius: 6,
      color: getColor(wave.hs),
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

// ============================
// ACTUALIZAR MAPA
// ============================

function updateMarkers() {
  markers.forEach(({ marker, loc }) => {
    const forecast = loc.forecast[selectedHour];
    const wave = getWaveData(forecast);

    marker.setStyle({
      color: getColor(wave.hs)
    });
  });
}

// ============================
// PANEL INFO
// ============================

function updateInfoPanel() {
  if (!selectedLocation) return;

  const forecast = selectedLocation.forecast[selectedHour];
  const wave = getWaveData(forecast);
  const wind = getWindData(forecast);

  infoPanel.innerHTML = `
    <h3>${selectedLocation.name}</h3>
    <p><b>Hora:</b> ${forecast.time}</p>
    <p><b>Hs:</b> ${wave.hs.toFixed(2)} m (${wave.source})</p>
    <p><b>Tp:</b> ${wave.tp?.toFixed(2) ?? "-"} s</p>
    <p><b>Dir:</b> ${wave.di?.toFixed(0) ?? "-"}°</p>
    <p><b>Viento:</b> ${wind.speed?.toFixed(1) ?? "-"} m/s</p>
    <p><b>Dir viento:</b> ${wind.direction?.toFixed(0) ?? "-"}°</p>
  `;
}

// ============================
// GRÁFICA
// ============================

function updateChart() {
  if (!selectedLocation) return;

  const labels = selectedLocation.forecast.map(f => f.time);

  const hs_main = selectedLocation.forecast.map(f => {
    const wave = getWaveData(f);
    return wave.hs;
  });

  const hs_pde = selectedLocation.forecast.map(f => f.hs_pde);
  const hs_cop = selectedLocation.forecast.map(f => f.hs);

  if (waveChart) waveChart.destroy();

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Oleaje (principal PDE→Copernicus)",
          data: hs_main,
          borderWidth: 2
        },
        {
          label: "PDE",
          data: hs_pde,
          borderDash: [5, 5]
        },
        {
          label: "Copernicus",
          data: hs_cop,
          borderDash: [2, 2]
        }
      ]
    },
    options: {
      responsive: true,
      interaction: {
        mode: "index",
        intersect: false
      },
      scales: {
        y: {
          title: {
            display: true,
            text: "Hs (m)"
          }
        }
      }
    }
  });
}

// ============================
// SLIDER
// ============================

hourSlider.max = FORECAST_HOURS - 1;

hourSlider.addEventListener("input", e => {
  selectedHour = parseInt(e.target.value);
  hourLabel.innerText = `+${selectedHour}h`;

  updateMarkers();
  updateInfoPanel();
});
