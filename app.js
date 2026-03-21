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
    Tp: ${point.tp} s<br>
    Dir: ${point.dir}°<br>
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
    <p><strong>Tp:</strong> ${point.tp} s</p>
    <p><strong>Direction:</strong> ${point.dir}°</p>
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

// Crear marcadores
function createMarkers() {
  markers = [];

  locations.forEach(site => {
    const point = getForecastPoint(site, selectedHour);
    const color = getStatusColor(point.wave, site.thresholds);

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

  // Ajustar vista del mapa a todos los puntos
  if (locations.length > 0) {
    const bounds = L.latLngBounds(locations.map(site => site.coords));
    map.fitBounds(bounds, { padding: [30, 30] });
  }
}

// Repinta todos los marcadores al cambiar la hora
function refreshMarker() {
  markers.forEach(({ marker, site }) => {
    const point = getForecastPoint(site, selectedHour);
    const color = getStatusColor(point.wave, site.thresholds);

    marker.setStyle({
      color,
      fillColor: color
    });

    marker.setPopupContent(getPopupContent(site, selectedHour));
  });

  if (selectedLocation) {
    updatePanel(selectedLocation, selectedHour);
    renderWaveChart(selectedLocation, selectedHour);
  }
}

// Cargar JSON
fetch("./wave_points.json")
  .then(response => {
    if (!response.ok) {
      throw new Error(`HTTP error ${response.status}`);
    }
    return response.json();
  })
  .then(data => {
    console.log("wave_points.json cargado:", data);

    locations = data.map(point => ({
     name: point.name,
      coords: [point.lat, point.lon],
      thresholds: { ...THRESHOLDS },
      forecast: point.forecast.map((f, i) => ({
        hour: i,
        wave: f.hs,
        tp: f.tp,
        dir: f.di
      }))
    }));

    createMarkers();
  })
  .catch(error => {
    console.error("Error cargando wave_points.json:", error);
  });

// Slider temporal
if (hourSlider && hourLabel) {
  hourSlider.max = FORECAST_HOURS - 1;
  hourSlider.value = selectedHour;
  hourLabel.textContent = selectedHour;

  hourSlider.addEventListener("input", event => {
    selectedHour = Number(event.target.value);
    hourLabel.textContent = selectedHour;
    refreshMarker();
  });
}
