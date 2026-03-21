const FORECAST_HOURS = 72;

// Umbrales para el punto
const THRESHOLDS = {
  greenMax: 1.0,
  orangeMax: 2.0
};

// Estado global
let selectedHour = 0;
let selectedLocation = null;
let waveChart = null;

// Referencias a elementos del DOM
const infoPanel = document.getElementById("info-panel");
const hourSlider = document.getElementById("hour-slider");
const hourLabel = document.getElementById("hour-label");
const waveChartCanvas = document.getElementById("wave-chart");

// Genera dirección aleatoria simple para pruebas
function randomDirection() {
  const dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW", "ENE", "ESE"];
  return dirs[Math.floor(Math.random() * dirs.length)];
}

// Genera una serie de 72 horas con valores ficticios
function generateForecast() {
  return Array.from({ length: FORECAST_HOURS }, (_, hour) => ({
    hour,
    wave: +(0.3 + Math.random() * 2.6).toFixed(1),
    wind: Math.floor(6 + Math.random() * 18),
    dir: randomDirection()
  }));
}

// ÚNICO punto de trabajo
const sitePoint = {
  name: "Valencia Port",
  coords: [39.448, -0.316],
  thresholds: { ...THRESHOLDS },
  forecast: generateForecast()
};

// Inicialización del mapa
const map = L.map("map").setView(sitePoint.coords, 9);

// Basemap
L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  attribution: "&copy; OpenStreetMap &copy; CARTO",
  subdomains: "abcd",
  maxZoom: 19
}).addTo(map);

// Calcula color según Hs y umbrales
function getStatusColor(wave, thresholds) {
  if (wave <= thresholds.greenMax) return "green";
  if (wave <= thresholds.orangeMax) return "orange";
  return "red";
}

// Convierte color en etiqueta
function getStatusLabel(color) {
  if (color === "green") return "Operational";
  if (color === "orange") return "Caution";
  return "High risk";
}

// Devuelve el forecast para la hora activa
function getForecastPoint(site, hourIndex) {
  return site.forecast[hourIndex];
}

// HTML del popup
function getPopupContent(site, hourIndex) {
  const point = getForecastPoint(site, hourIndex);
  const color = getStatusColor(point.wave, site.thresholds);
  const label = getStatusLabel(color);

  return `
    <strong>${site.name}</strong><br>
    Forecast: +${point.hour}h<br>
    Wave: ${point.wave} m<br>
    Wind: ${point.wind} kt ${point.dir}<br>
    Status: <span style="color:${color}; font-weight:600;">${label}</span>
  `;
}

// Actualiza panel lateral
function updatePanel(site, hourIndex) {
  const point = getForecastPoint(site, hourIndex);
  const color = getStatusColor(point.wave, site.thresholds);
  const label = getStatusLabel(color);

  infoPanel.innerHTML = `
    <p><strong>Name:</strong> ${site.name}</p>
    <p><strong>Forecast hour:</strong> +${point.hour}h</p>
    <p><strong>Wave:</strong> ${point.wave} m</p>
    <p><strong>Wind:</strong> ${point.wind} kt ${point.dir}</p>
    <p><strong>Status:</strong> <span style="color:${color}; font-weight:600;">${label}</span></p>
    <p><strong>Thresholds:</strong> green ≤ ${site.thresholds.greenMax} m, orange ≤ ${site.thresholds.orangeMax} m, red > ${site.thresholds.orangeMax} m</p>
  `;
}

// Dibuja la serie temporal de Hs
function renderWaveChart(site, hourIndex) {
  if (!waveChartCanvas || typeof Chart === "undefined") return;

  const labels = site.forecast.map(point => point.hour);
  const values = site.forecast.map(point => point.wave);

  const pointColors = site.forecast.map((point, index) =>
    index === hourIndex ? "red" : "#2563eb"
  );

  const pointRadii = site.forecast.map((point, index) =>
    index === hourIndex ? 5 : 2
  );

  if (waveChart) {
    waveChart.destroy();
  }

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Hs (m)",
          data: values,
          borderColor: "#2563eb",
          backgroundColor: "rgba(37, 99, 235, 0.12)",
          fill: true,
          tension: 0.25,
          pointBackgroundColor: pointColors,
          pointRadius: pointRadii,
          pointHoverRadius: pointRadii
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: true
        }
      },
      scales: {
        x: {
          title: {
            display: true,
            text: "Forecast hour"
          }
        },
        y: {
          title: {
            display: true,
            text: "Hs (m)"
          },
          beginAtZero: true
        }
      }
    }
  });
}

// Crear único marcador
const initialPoint = getForecastPoint(sitePoint, selectedHour);
const initialColor = getStatusColor(initialPoint.wave, sitePoint.thresholds);

const marker = L.circleMarker(sitePoint.coords, {
  radius: 6,
  color: initialColor,
  fillColor: initialColor,
  fillOpacity: 0.8,
  weight: 2
}).addTo(map);

marker.bindPopup(getPopupContent(sitePoint, selectedHour));

marker.on("click", () => {
  selectedLocation = sitePoint;
  updatePanel(sitePoint, selectedHour);
  renderWaveChart(sitePoint, selectedHour);
});

// Repinta el marcador al cambiar la hora
function refreshMarker() {
  const point = getForecastPoint(sitePoint, selectedHour);
  const color = getStatusColor(point.wave, sitePoint.thresholds);

  marker.setStyle({
    color,
    fillColor: color
  });

  marker.setPopupContent(getPopupContent(sitePoint, selectedHour));

  if (selectedLocation) {
    updatePanel(sitePoint, selectedHour);
    renderWaveChart(sitePoint, selectedHour);
  }
}

// Slider temporal
if (hourSlider && hourLabel) {
  hourSlider.max = FORECAST_HOURS - 1;
  hourSlider.value = selectedHour;
  hourLabel.textContent = selectedHour;

  hourSlider.addEventListener("input", (event) => {
    selectedHour = Number(event.target.value);
    hourLabel.textContent = selectedHour;
    refreshMarker();
  });
}
