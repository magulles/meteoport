const FORECAST_HOURS = 72;

// Umbrales comunes para todos los puertos, de momento
const COMMON_THRESHOLDS = {
  greenMax: 1.0,
  orangeMax: 2.0
};

// Variable global: hora seleccionada en la predicción
let selectedHour = 0;
let selectedLocation = null;

// Referencias a elementos del DOM
const infoPanel = document.getElementById("info-panel");
const hourSlider = document.getElementById("hour-slider");
const hourLabel = document.getElementById("hour-label");

// Genera dirección aleatoria simple para pruebas
function randomDirection() {
  const dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW", "ENE", "ESE"];
  return dirs[Math.floor(Math.random() * dirs.length)];
}

// Genera una serie de 72 horas con valores ficticios
function generateForecast() {
  return Array.from({ length: FORECAST_HOURS }, (_, hour) => ({
    hour,
    wave: +(0.3 + Math.random() * 2.6).toFixed(1), // entre 0.3 y 2.9 m aprox
    wind: Math.floor(6 + Math.random() * 18),      // entre 6 y 23 kt aprox
    dir: randomDirection()
  }));
}

// Datos iniciales de puertos
const locations = [
  {
    name: "Valencia Port",
    coords: [39.448, -0.316],
    thresholds: { ...COMMON_THRESHOLDS },
    forecast: generateForecast()
  },
  {
    name: "Sagunto Port",
    coords: [39.641, -0.214],
    thresholds: { ...COMMON_THRESHOLDS },
    forecast: generateForecast()
  },
  {
    name: "Gandia Port",
    coords: [38.995, -0.153],
    thresholds: { ...COMMON_THRESHOLDS },
    forecast: generateForecast()
  }
];

// Inicialización del mapa
const map = L.map("map").setView([39.35, -0.25], 9);

// Basemap más limpio
L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  attribution: "&copy; OpenStreetMap &copy; CARTO",
  subdomains: "abcd",
  maxZoom: 19
}).addTo(map);

// Calcula color según hs y umbrales
function getStatusColor(wave, thresholds) {
  if (wave <= thresholds.greenMax) return "green";
  if (wave <= thresholds.orangeMax) return "orange";
  return "red";
}

// Convierte color en etiqueta de estado
function getStatusLabel(color) {
  if (color === "green") return "Operational";
  if (color === "orange") return "Caution";
  return "High risk";
}

// Devuelve el forecast de una localización para la hora activa
function getForecastPoint(location, hourIndex) {
  return location.forecast[hourIndex];
}

// HTML del popup
function getPopupContent(location, hourIndex) {
  const point = getForecastPoint(location, hourIndex);
  const color = getStatusColor(point.wave, location.thresholds);
  const label = getStatusLabel(color);

  return `
    <strong>${location.name}</strong><br>
    Forecast: +${point.hour}h<br>
    Wave: ${point.wave} m<br>
    Wind: ${point.wind} kt ${point.dir}<br>
    Status: <span style="color:${color}; font-weight:600;">${label}</span>
  `;
}

// Actualiza panel lateral
function updatePanel(location, hourIndex) {
  const point = getForecastPoint(location, hourIndex);
  const color = getStatusColor(point.wave, location.thresholds);
  const label = getStatusLabel(color);

  infoPanel.innerHTML = `
    <p><strong>Name:</strong> ${location.name}</p>
    <p><strong>Forecast hour:</strong> +${point.hour}h</p>
    <p><strong>Wave:</strong> ${point.wave} m</p>
    <p><strong>Wind:</strong> ${point.wind} kt ${point.dir}</p>
    <p><strong>Status:</strong> <span style="color:${color}; font-weight:600;">${label}</span></p>
    <p><strong>Thresholds:</strong> green ≤ ${location.thresholds.greenMax} m, orange ≤ ${location.thresholds.orangeMax} m, red > ${location.thresholds.orangeMax} m</p>
  `;
}

// Guardamos referencias a marcadores para poder repintarlos
const markers = [];

// Crear marcadores iniciales
locations.forEach((location) => {
  const point = getForecastPoint(location, selectedHour);
  const color = getStatusColor(point.wave, location.thresholds);

  const marker = L.circleMarker(location.coords, {
    radius: 5,
    color,
    fillColor: color,
    fillOpacity: 0.8,
    weight: 2
  }).addTo(map);

  marker.bindPopup(getPopupContent(location, selectedHour));

  marker.on("click", () => {
    selectedLocation = location;
    updatePanel(location, selectedHour);
  });

  markers.push({ marker, location });
});

// Repinta todos los marcadores al cambiar la hora
function refreshMarkers() {
  markers.forEach(({ marker, location }) => {
    const point = getForecastPoint(location, selectedHour);
    const color = getStatusColor(point.wave, location.thresholds);

    marker.setStyle({
      color,
      fillColor: color
    });

    marker.setPopupContent(getPopupContent(location, selectedHour));
  });
}

// Slider temporal
if (hourSlider && hourLabel) {
  hourSlider.addEventListener("input", (event) => {
    selectedHour = Number(event.target.value);
    hourLabel.textContent = selectedHour;

    refreshMarkers();

    if (selectedLocation) {
      updatePanel(selectedLocation, selectedHour);
    }
  });
}
