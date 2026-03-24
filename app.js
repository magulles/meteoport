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

function dirToArrow(deg) {
  if (deg === null || deg === undefined || Number.isNaN(deg)) return null;

  const arrows = ["↑", "↗", "→", "↘", "↓", "↙", "←", "↖"];
  const idx = Math.round((((deg % 360) + 360) % 360) / 45) % 8;
  return arrows[idx];
}

// PDE -> Copernicus
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
      tpCopernicus: f.tp ?? null,
      dirCopernicus: f.di ?? null,
      wavePde: f.hs_pde ?? null,
      tpPde: f.tp_pde ?? null,
      dirPde: f.di_pde ?? null,
      windSpeed: f.wind_speed_10m_ms ?? null,
      windDir: f.wind_direction_10m_deg ?? null
    };
  });
}

function getColor(hs) {
  if (hs === null || hs === undefined || Number.isNaN(hs)) return "#9ca3af";
  if (hs <= THRESHOLDS.greenMax) return "green";
  if (hs <= THRESHOLDS.orangeMax) return "orange";
  return "red";
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

    if (hourSlider) {
      const maxAvailable = Math.max(0, locations[0].forecast.length - 1);
      hourSlider.max = maxAvailable;
      hourSlider.value = selectedHour;
    }

    initMarkers();
    updateHourLabel();
  })
  .catch(err => {
    console.error(err);
    if (infoPanel) {
      infoPanel.innerHTML = `
        <p><strong>Error cargando datos</strong></p>
        <p>${err.message}</p>
      `;
    }
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

    marker.on("click", () => {
      selectedLocation = loc;
      updateInfoPanel();
      updateChart();
    });

    marker.bindTooltip(loc.name, {
      direction: "top",
      offset: [0, -6]
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
// PANEL INFO
// ============================

function updateInfoPanel() {
  if (!selectedLocation || !infoPanel) return;

  const f = selectedLocation.forecast[selectedHour];
  if (!f) {
    infoPanel.innerHTML = `
      <p><strong>Name:</strong> ${selectedLocation.name}</p>
      <p><strong>No data for this time</strong></p>
    `;
    return;
  }

  const arrow = dirToArrow(f.dir);

  infoPanel.innerHTML = `
    <h3>${selectedLocation.name}</h3>
    <p><b>Hora:</b> ${f.time ?? "--"}</p>
    <p><b>Hs:</b> ${formatNumber(f.wave)} m (${f.waveSource})</p>
    <p><b>Tp:</b> ${formatNumber(f.tp)} s</p>
    <p><b>Dir:</b> ${formatNumber(f.dir)}° ${arrow ? `(${arrow})` : ""}</p>
    <p><b>Viento:</b> ${formatNumber(f.windSpeed)} m/s</p>
    <p><b>Dir viento:</b> ${formatNumber(f.windDir)}°</p>
  `;
}

// ============================
// GRÁFICA
// ============================

function updateChart() {
  if (!selectedLocation || !waveChartCanvas) return;

  const labels = selectedLocation.forecast.map(f => formatTimeLabel(f.time));

  const hsPde = selectedLocation.forecast.map(f => f.wavePde);
  const hsCop = selectedLocation.forecast.map(f => f.waveCopernicus);

  const tpPde = selectedLocation.forecast.map(f => f.tpPde);
  const tpCop = selectedLocation.forecast.map(f => f.tpCopernicus);

  const dirArrowSeries = selectedLocation.forecast.map(f => {
    if (f.dirPde !== null && f.dirPde !== undefined && !Number.isNaN(f.dirPde)) return 2;
    if (f.dirCopernicus !== null && f.dirCopernicus !== undefined && !Number.isNaN(f.dirCopernicus)) return 1;
    return null;
  });

  const dirArrowLabels = selectedLocation.forecast.map(f => {
    const dirValue =
      f.dirPde !== null && f.dirPde !== undefined && !Number.isNaN(f.dirPde)
        ? f.dirPde
        : f.dirCopernicus;

    return dirToArrow(dirValue);
  });

  const selectedIndex = selectedHour;

  if (waveChart) waveChart.destroy();

  const directionArrowsPlugin = {
    id: "directionArrowsPlugin",
    afterDatasetsDraw(chart) {
      const dirDatasetIndex = chart.data.datasets.findIndex(ds => ds.customDirectionRow === true);
      if (dirDatasetIndex === -1) return;

      const meta = chart.getDatasetMeta(dirDatasetIndex);
      const ctx = chart.ctx;

      ctx.save();
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.font = "16px Arial";

      meta.data.forEach((point, i) => {
        const arrow = dirArrowLabels[i];
        if (!arrow || !point) return;

        ctx.fillStyle = i === selectedIndex ? "#111827" : "#6b7280";
        ctx.fillText(arrow, point.x, point.y);
      });

      ctx.restore();
    }
  };

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "PdE",
          data: hsPde,
          yAxisID: "yHs",
          borderColor: "#ef4444",
          backgroundColor: "transparent",
          borderWidth: 2.2,
          pointRadius: 0,
          pointHoverRadius: 3,
          tension: 0.25,
          spanGaps: true
        },
        {
          label: "Copernicus",
          data: hsCop,
          yAxisID: "yHs",
          borderColor: "#2563eb",
          backgroundColor: "transparent",
          borderWidth: 2,
          borderDash: [6, 4],
          pointRadius: 0,
          pointHoverRadius: 3,
          tension: 0.25,
          spanGaps: true
        },
        {
          label: "Tp PdE",
          data: tpPde,
          yAxisID: "yTp",
          borderColor: "#f59e0b",
          backgroundColor: "transparent",
          borderWidth: 1.6,
          pointRadius: 0,
          pointHoverRadius: 2,
          tension: 0.2,
          spanGaps: true
        },
        {
          label: "Tp Copernicus",
          data: tpCop,
          yAxisID: "yTp",
          borderColor: "#10b981",
          backgroundColor: "transparent",
          borderWidth: 1.4,
          borderDash: [4, 3],
          pointRadius: 0,
          pointHoverRadius: 2,
          tension: 0.2,
          spanGaps: true
        },
        {
          label: "Dirección",
          data: dirArrowSeries,
          yAxisID: "yDir",
          borderColor: "transparent",
          backgroundColor: "transparent",
          pointRadius: 0,
          pointHoverRadius: 0,
          spanGaps: true,
          customDirectionRow: true
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
            filter: (legendItem) => {
              return legendItem.text === "PdE" || legendItem.text === "Copernicus";
            }
          }
        },
        tooltip: {
          callbacks: {
            afterBody: (items) => {
              if (!items.length) return "";

              const idx = items[0].dataIndex;
              const f = selectedLocation.forecast[idx];
              const arrow =
                dirToArrow(
                  f.dirPde !== null && f.dirPde !== undefined && !Number.isNaN(f.dirPde)
                    ? f.dirPde
                    : f.dirCopernicus
                ) || "-";

              const tpShown =
                f.tpPde !== null && f.tpPde !== undefined && !Number.isNaN(f.tpPde)
                  ? `Tp PdE: ${formatNumber(f.tpPde)} s`
                  : `Tp Copernicus: ${formatNumber(f.tpCopernicus)} s`;

              return [`${tpShown}`, `Dirección: ${arrow}`];
            }
          }
        }
      },
      scales: {
        x: {
          ticks: {
            maxTicksLimit: 16,
            maxRotation: 55,
            minRotation: 55
          },
          grid: {
            color: "#eef2f7"
          }
        },
        yHs: {
          type: "linear",
          position: "left",
          title: {
            display: true,
            text: "Hs (m)"
          },
          beginAtZero: true,
          grid: {
            color: "#e5e7eb"
          }
        },
        yTp: {
          type: "linear",
          position: "right",
          title: {
            display: true,
            text: "Tp (s)"
          },
          grid: {
            drawOnChartArea: false
          }
        },
        yDir: {
          min: 0,
          max: 3,
          display: false,
          grid: {
            drawOnChartArea: false
          }
        }
      }
    },
    plugins: [directionArrowsPlugin]
  });
}

// ============================
// SLIDER
// ============================

if (hourSlider) {
  hourSlider.addEventListener("input", e => {
    selectedHour = parseInt(e.target.value, 10);

    updateMarkers();
    updateInfoPanel();
    updateChart();
    updateHourLabel();
  });
}

function updateHourLabel() {
  if (!hourLabel || !locations.length) return;

  const refLocation = selectedLocation || locations[0];
  const f = refLocation?.forecast?.[selectedHour];

  hourLabel.innerText = f?.time ? formatTimeLabel(f.time) : "--";
}
