// ============================
// CONFIG
// ============================

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

// ============================
// CARGA DATOS
// ============================

fetch("./meteo_points.json")
  .then(res => {
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} cargando meteo_points.json`);
    }
    return res.json();
  })
  .then(data => {
    const rawPoints = Array.isArray(data) ? data : data.points;

    locations = rawPoints.map(point => ({
      name: point.name,
      coords: [point.lat, point.lon],
      thresholds: { ...THRESHOLDS },
      forecast: buildMergedForecast(point)
    }));

    if (!locations.length) {
      throw new Error("No hay puntos en meteo_points.json");
    }

    const maxHour = Math.max(0, getForecastLength() - 1);
    hourSlider.max = maxHour;
    hourSlider.value = selectedHour;

    initMarkers();
    updateHourLabel();
  })
  .catch(err => {
    console.error(err);
    infoPanel.innerHTML = `
      <p><strong>Error cargando datos</strong></p>
      <p>${err.message}</p>
    `;
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
      radius: 6,
      color,
      fillColor: color,
      fillOpacity: 0.85,
      weight: 2
    }).addTo(map);

    marker.bindTooltip(loc.name, {
      direction: "top",
      offset: [0, -6]
    });

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

    marker.setStyle({
      color,
      fillColor: color
    });
  });
}

// ============================
// PANEL
// ============================

function updateInfoPanel() {
  if (!selectedLocation) return;

  const f = selectedLocation.forecast[selectedHour];
  if (!f) {
    infoPanel.innerHTML = `
      <p><strong>Name:</strong> ${selectedLocation.name}</p>
      <p><strong>No data for this time</strong></p>
    `;
    return;
  }

  const statusColor = getColor(f.wave);

  infoPanel.innerHTML = `
    <h3>${selectedLocation.name}</h3>
    <p><strong>Time:</strong> ${formatTimeLabel(f.time)}</p>
    <p><strong>Hs:</strong> ${formatNumber(f.wave)} m (${f.waveSource})</p>
    <p><strong>Tp:</strong> ${formatNumber(f.tp)} s</p>
    <p><strong>Wave direction:</strong> ${formatNumber(f.dir)}°</p>
    <p><strong>Wind:</strong> ${formatNumber(f.windSpeed)} m/s</p>
    <p><strong>Wind direction:</strong> ${formatNumber(f.windDir)}°</p>
    <p><strong>Status:</strong> <span style="color:${statusColor}; font-weight:700;">${statusColor.toUpperCase()}</span></p>
  `;
}

// ============================
// CHART PLUGIN: VERTICAL LINE
// ============================

const verticalCursorPlugin = {
  id: "verticalCursorPlugin",
  afterDraw(chart, args, options) {
    const selectedIndex = options?.selectedIndex ?? 0;
    const xScale = chart.scales.x;
    const yScale = chart.scales.y;

    if (!xScale || !yScale) return;
    if (selectedIndex < 0 || selectedIndex >= chart.data.labels.length) return;

    const x = xScale.getPixelForValue(selectedIndex);
    const topY = chart.chartArea.top;
    const bottomY = chart.chartArea.bottom;
    const ctx = chart.ctx;

    ctx.save();
    ctx.beginPath();
    ctx.moveTo(x, topY);
    ctx.lineTo(x, bottomY);
    ctx.lineWidth = 1.5;
    ctx.strokeStyle = "#9ca3af";
    ctx.stroke();
    ctx.restore();
  }
};

// ============================
// GRÁFICA
// ============================

function renderChart() {
  if (!selectedLocation || !waveChartCanvas) return;

  const forecast = selectedLocation.forecast;

  const labels = forecast.map(f => formatTimeLabel(f.time));
  const hsPde = forecast.map(f => f.wavePde);
  const hsCop = forecast.map(f => f.waveCopernicus);

  if (waveChart) {
    waveChart.destroy();
  }

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "PdE",
          data: hsPde,
          borderColor: "#ef4444",
          backgroundColor: "transparent",
          borderWidth: 2.2,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.25,
          spanGaps: true
        },
        {
          label: "Copernicus",
          data: hsCop,
          borderColor: "#2563eb",
          backgroundColor: "transparent",
          borderWidth: 2,
          borderDash: [6, 4],
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.25,
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
      },
      plugins: {
        legend: {
          labels: {
            filter: (item) => item.text === "PdE" || item.text === "Copernicus"
          }
        },
        tooltip: {
          callbacks: {
            title: (items) => {
              if (!items.length) return "";
              const idx = items[0].dataIndex;
              return forecast[idx]?.time || "";
            },
            label: () => "",
            afterBody: (items) => {
              if (!items.length) return [];

              const idx = items[0].dataIndex;
              const f = forecast[idx];

              return [
                `Hs Copernicus: ${formatNumber(f.waveCopernicus)} m`,
                `Hs PdE: ${formatNumber(f.wavePde)} m`,
                `Tp PdE: ${formatNumber(f.tpPde)} s`,
                `Di PdE: ${formatNumber(f.dirPde, 0)}°`
              ];
            }
          }
        },
        verticalCursorPlugin: {
          selectedIndex: selectedHour
        }
      },
      scales: {
        x: {
          grid: {
            color: "#eef2f7"
          },
          ticks: {
            maxTicksLimit: 16,
            maxRotation: 55,
            minRotation: 55
          }
        },
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: "Hs (m)"
          },
          grid: {
            color: "#e5e7eb"
          }
        }
      }
    },
    plugins: [verticalCursorPlugin]
  });
}

function updateChartCursorOnly() {
  if (!waveChart) return;
  waveChart.options.plugins.verticalCursorPlugin.selectedIndex = selectedHour;
  waveChart.update("none");
}

// ============================
// SLIDER
// ============================

hourSlider.addEventListener("input", e => {
  selectedHour = parseInt(e.target.value, 10);

  updateMarkers();
  updateInfoPanel();
  updateHourLabel();
  updateChartCursorOnly();
});

function updateHourLabel() {
  if (!locations.length) {
    hourLabel.innerText = "--";
    return;
  }

  const refLocation = selectedLocation || locations[0];
  const f = refLocation?.forecast?.[selectedHour];

  hourLabel.innerText = f?.time ? formatTimeLabel(f.time) : "--";
}
