// ============================
// RESIZE HANDLER (MAP + CHART)
// ============================

window.addEventListener("load", () => {
  setupResponsiveFixes();
});

function setupResponsiveFixes() {
  let resizeTimeout;

  function handleResize() {
    clearTimeout(resizeTimeout);

    resizeTimeout = setTimeout(() => {
      if (window.map) {
        window.map.invalidateSize();
      }

      if (window.chart) {
        window.chart.resize();
      }
    }, 200);
  }

  window.addEventListener("resize", handleResize);

  window.addEventListener("orientationchange", () => {
    setTimeout(() => {
      handleResize();
    }, 300);
  });
}

// TOUCH FIX
document.addEventListener("touchstart", function () {}, { passive: true });


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
map.createPane("routesPane");
map.getPane("routesPane").style.zIndex = 350;

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

function formatDateTimeLong(isoTime) {
  if (!isoTime) return "--";

  const d = new Date(isoTime);
  const months = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"];

  const year = d.getUTCFullYear();
  const month = months[d.getUTCMonth()];
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");
  const min = String(d.getUTCMinutes()).padStart(2, "0");

  return `${day} ${month} ${year} ${hour}:${min} UTC`;
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

function escapeHtml(text) {
  return String(text ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function getOperationalWave(f) {
  const hasPde = f && f.hs_pde !== null && f.hs_pde !== undefined && !Number.isNaN(f.hs_pde);
  const hasPort = f && f.hs_port !== null && f.hs_port !== undefined && !Number.isNaN(f.hs_port);

  if (hasPde) {
    return {
      wave: f.hs_pde,
      tp: f.tp_pde,
      dir: f.di_pde,
      source: "PdE"
    };
  }

  if (hasPort) {
    return {
      wave: f.hs_port,
      tp: null,
      dir: null,
      source: "Puerto"
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
      wavePort: f.hs_port ?? null,
      waveCopernicus: f.hs ?? null,
      waveObs: f.hsobs ?? null,
      tpPde: f.tp_pde ?? null,
      tpCopernicus: f.tp ?? null,
      dirPde: f.di_pde ?? null,
      dirCopernicus: f.di ?? null,
      windSpeed: f.wind_speed_10m_ms ?? null,
      windDir: f.wind_direction_10m_deg ?? null,
      windObs: f.wspdobs ?? null
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
// RUTAS
// ============================

function buildRoutes(rawRoutes) {
  return (rawRoutes || []).map(route => {
    const resolvedPoints = (route.points || [])
      .map(pointName => {
        const loc = findLocationByName(pointName);
        return {
          name: pointName,
          loc
        };
      });

    const validLocations = resolvedPoints
      .filter(p => p.loc)
      .map(p => p.loc);

    return {
      ...route,
      resolvedPoints,
      locations: validLocations
    };
  });
}

function calculateRouteSummary(route) {
  if (!route || !route.locations || !route.locations.length) {
    return { hasData: false, reason: "Ruta sin puntos válidos" };
  }

  const startMs = new Date(route.departure_time).getTime();
  const endMs = new Date(route.arrival_time).getTime();

  if (Number.isNaN(startMs) || Number.isNaN(endMs) || endMs < startMs) {
    return { hasData: false, reason: "Ventana temporal inválida" };
  }

  let best = null;
  let recordsInWindow = 0;

  route.locations.forEach(loc => {
    (loc.forecast || []).forEach(f => {
      const t = new Date(f.time).getTime();
      if (Number.isNaN(t)) return;
      if (t < startMs || t > endMs) return;

      recordsInWindow += 1;

      if (f.wave === null || f.wave === undefined || Number.isNaN(f.wave)) return;

      if (!best || f.wave > best.wave) {
        best = {
          locationName: loc.name,
          time: f.time,
          wave: f.wave,
          tp: f.tp,
          dir: f.dir,
          waveSource: f.waveSource
        };
      }
    });
  });

  if (!recordsInWindow) {
    return { hasData: false, reason: "No hay datos en la ventana temporal de la ruta" };
  }

  if (!best) {
    return { hasData: false, reason: "Hay registros en la ventana, pero sin oleaje válido" };
  }

  return { hasData: true, ...best };
}

function getRouteDisplayColor(route) {
  const summary = calculateRouteSummary(route);
  if (!summary.hasData) return "#64748b";
  return getHexColorFromHs(summary.wave);
}

function updateRouteStyles() {
  routeLayers.forEach(({ route, polyline }) => {
    const isSelected = selectedRoute && selectedRoute.id === route.id;
    const color = getRouteDisplayColor(route);

    polyline.setStyle({
      color,
      weight: isSelected ? 5 : 3,
      opacity: isSelected ? 0.95 : 0.75
    });
  });
}

function initRoutes() {
  routeLayers.forEach(({ polyline }) => map.removeLayer(polyline));
  routeLayers = [];

  routes.forEach(route => {
    const latlngs = route.locations.map(loc => loc.coords);
    if (latlngs.length < 2) return;

    const polyline = L.polyline(latlngs, {
      color: getRouteDisplayColor(route),
      weight: 3,
      opacity: 0.75
    }).addTo(map);
    polyline.bringToBack();
    polyline.bindTooltip(route.name, { direction: "top", sticky: true });

    polyline.on("click", () => {
      selectedRoute = route;
      selectedLocation = null;
      updateRouteStyles();
      updateInfoPanel();
    });

    routeLayers.push({ route, polyline });
  });

  updateRouteStyles();
}

// ============================
// CARGA DATOS
// ============================

Promise.all([
  fetch("./meteo_points_tot.json").then(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status} cargando meteo_points_tot.json`);
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

    if (!locations.length) {
      throw new Error("No hay puntos en meteo_points_tot.json");
    }

    routes = buildRoutes(routesData);

    const maxHour = Math.max(0, getForecastLength() - 1);
    hourSlider.max = maxHour;
    hourSlider.value = selectedHour;

    initMarkers();
    initRoutes();
    updateHourLabel();
  })
  .catch(err => {
    console.error(err);
    infoPanel.innerHTML = `
      <p><strong>Error cargando datos</strong></p>
      <p>${escapeHtml(err.message)}</p>
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
      radius: 4,
      color,
      fillColor: color,
      fillOpacity: 0.85,
      weight: 2
    }).addTo(map);

    marker.bindTooltip(loc.name, { direction: "top", offset: [0, -6] });

    marker.on("click", () => {
      selectedLocation = loc;
      selectedRoute = null;
      updateRouteStyles();
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

function renderLocationInfoPanel() {
  if (!selectedLocation) return;

  const f = selectedLocation.forecast[selectedHour];

  if (!f) {
    infoPanel.innerHTML = `
      <p><strong>Name:</strong> ${escapeHtml(selectedLocation.name)}</p>
      <p><strong>No data for this time</strong></p>
    `;
    return;
  }

  const statusColor = getColor(f.wave);

  infoPanel.innerHTML = `
    <h3>${escapeHtml(selectedLocation.name)}</h3>
    <p><strong>Time:</strong> ${formatTimeLabel(f.time)}</p>
    <p><strong>Hs:</strong> ${formatNumber(f.wave)} m (${escapeHtml(f.waveSource)})</p>
    <p><strong>Tp:</strong> ${formatNumber(f.tp)} s</p>
    <p><strong>Wave direction:</strong> ${formatNumber(f.dir)}°</p>
    <p><strong>Wind:</strong> ${formatNumber(f.windSpeed)} m/s</p>
    <p><strong>Wind direction:</strong> ${formatNumber(f.windDir)}°</p>
    <p><strong>Status:</strong> <span style="color:${statusColor}; font-weight:700;">${statusColor.toUpperCase()}</span></p>
  `;
}

function renderRouteInfoPanel() {
  if (!selectedRoute) return;

  const summary = calculateRouteSummary(selectedRoute);
  const missingPoints = selectedRoute.resolvedPoints.filter(p => !p.loc).map(p => p.name);
  const pointsLabel = selectedRoute.resolvedPoints.map(p => p.name).join(" → ");

  if (!summary.hasData) {
    infoPanel.innerHTML = `...`;
    return;
  }

  const statusColor = getColor(summary.wave);

  infoPanel.innerHTML = `
    <h3>${escapeHtml(selectedRoute.name)}</h3>
    <p><strong>Salida:</strong> ${formatDateTimeLong(selectedRoute.departure_time)}</p>
    <p><strong>Llegada:</strong> ${formatDateTimeLong(selectedRoute.arrival_time)}</p>
    <p><strong>Puntos:</strong> ${escapeHtml(pointsLabel)}</p>
    <hr style="margin:10px 0;">
    <p><strong>Hsmax ruta:</strong> ${formatNumber(summary.wave)} m (${escapeHtml(summary.waveSource)})</p>
    <p><strong>Tp asociado:</strong> ${formatNumber(summary.tp)} s</p>
    <p><strong>Dirección asociada:</strong> ${formatNumber(summary.dir)}°</p>
    <p><strong>Ocurre en:</strong> ${escapeHtml(summary.locationName)}</p>
    <p><strong>Hora:</strong> ${formatDateTimeLong(summary.time)}</p>
    <p><strong>Estado:</strong> <span style="color:${statusColor}; font-weight:700;">${statusColor.toUpperCase()}</span></p>
  `;
}

function updateInfoPanel() {
  if (selectedRoute) {
    renderRouteInfoPanel();
    return;
  }

  if (selectedLocation) {
    renderLocationInfoPanel();
    return;
  }

  infoPanel.innerHTML = `<p><strong>Selecciona un punto o una ruta</strong></p>`;
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
// CHART PLUGIN: PDE WAVE ARROWS 
// ============================

const pdeWaveArrowsPlugin = {
  id: "pdeWaveArrowsPlugin",
  afterDatasetsDraw(chart, args, options) {
    const datasetIndex = options?.datasetIndex ?? 2;
    const directions = options?.directions ?? [];
    const topPaddingPx = options?.topPaddingPx ?? 18;
    const arrowYValue = options?.arrowYValue ?? null;
    const arrowLengthPx = options?.arrowLengthPx ?? 14;
    const arrowHeadPx = options?.arrowHeadPx ?? 5;
    const lineWidth = options?.lineWidth ?? 1.4;
    const color = options?.color ?? "#6b7280";

    const meta = chart.getDatasetMeta(datasetIndex);
    const dataset = chart.data.datasets?.[datasetIndex];
    const ctx = chart.ctx;
    const chartArea = chart.chartArea;
    const yScale = chart.scales.y;

    if (!meta || !dataset || meta.hidden) return;
    if (!meta.data || !meta.data.length) return;
    if (!chartArea || !yScale) return;

    ctx.save();
    ctx.strokeStyle = color;
    ctx.fillStyle = color;
    ctx.lineWidth = lineWidth;

    const yFixed = arrowYValue !== null
      ? yScale.getPixelForValue(arrowYValue)
      : chartArea.top + topPaddingPx;

    meta.data.forEach((pointEl, i) => {
      const hsValue = dataset.data[i];
      const dirFrom = directions[i];

      if (hsValue === null || hsValue === undefined || Number.isNaN(hsValue)) return;
      if (dirFrom === null || dirFrom === undefined || Number.isNaN(dirFrom)) return;

      const x = pointEl.x;
      const y = yFixed;

      const arrowBearing = (dirFrom + 180) % 360;
      const rad = arrowBearing * Math.PI / 180;
      const dx = arrowLengthPx * Math.sin(rad);
      const dy = -arrowLengthPx * Math.cos(rad);

      const x1 = x - dx / 2;
      const y1 = y - dy / 2;
      const x2 = x + dx / 2;
      const y2 = y + dy / 2;

      ctx.beginPath();
      ctx.moveTo(x1, y1);
      ctx.lineTo(x2, y2);
      ctx.stroke();

      const angle = Math.atan2(y2 - y1, x2 - x1);
      const a1 = angle + Math.PI * 0.82;
      const a2 = angle - Math.PI * 0.82;

      ctx.beginPath();
      ctx.moveTo(x2, y2);
      ctx.lineTo(x2 + arrowHeadPx * Math.cos(a1), y2 + arrowHeadPx * Math.sin(a1));
      ctx.moveTo(x2, y2);
      ctx.lineTo(x2 + arrowHeadPx * Math.cos(a2), y2 + arrowHeadPx * Math.sin(a2));
      ctx.stroke();
    });

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
  const hsPort = forecast.map(f => f.wavePort);
  const hsPde = forecast.map(f => f.wavePde);
  const hsCop = forecast.map(f => f.waveCopernicus);
  const hsObs = forecast.map(f => f.waveObs);
  const dirPde = forecast.map(f => f.dirPde);
  const dirCop = forecast.map(f => f.dirCopernicus);

  if (waveChart) {
    waveChart.destroy();
  }

const daySeparatorPlugin = {
  id: 'daySeparatorPlugin',
  afterDraw(chart) {
    const { ctx, chartArea, scales } = chart;
    const xScale = scales.x;
    const labels = chart.data.labels || [];

    if (!xScale || !labels.length) return;

    ctx.save();
    ctx.strokeStyle = 'rgba(70, 70, 70, 0.25)';
    ctx.lineWidth = 1.2;
    ctx.setLineDash([4, 4]);

    for (let i = 1; i < labels.length; i++) {
      const prev = labels[i - 1];
      const curr = labels[i];

      const prevDay = prev.split('-')[1];
      const currDay = curr.split('-')[1];

      if (prevDay !== currDay) {
        const x = xScale.getPixelForValue(i);

        ctx.beginPath();
        ctx.moveTo(x, chartArea.top);
        ctx.lineTo(x, chartArea.bottom);
        ctx.stroke();
      }
    }

    ctx.restore();
  }
};

const allHs = [
  ...hsPort,
  ...hsPde,
  ...hsCop,
  ...hsObs
].filter(v => v != null && !Number.isNaN(v));

const maxHs = Math.max(...allHs);
const arrowYValue = maxHs + 0.3;   // donde van las flechas
const yMaxChart = maxHs + 0.7;     // techo del gráfico  

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Puerto",
          data: hsPort,
          borderColor: "#16a34a",
          backgroundColor: "transparent",
          borderWidth: 2.2,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.25,
          spanGaps: true
        },
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
        },
{
  label: "Obs",
  data: hsObs,
  borderColor: "rgba(0,0,0,0.6)",   
  backgroundColor: "rgba(0,0,0,0.3)",
  borderWidth: 1.2,                 
  pointRadius: 1.5,                   
  pointHoverRadius: 3,
  tension: 0.2,
  borderDash: [],              
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
    tooltip: {
      callbacks: {
        title: items => {
          if (!items.length) return "";
          const idx = items[0].dataIndex;
          return forecast[idx]?.time || "";
        },
        label: () => "",
        afterBody: items => {
          if (!items.length) return [];

          const idx = items[0].dataIndex;
          const f = forecast[idx];
          return [
            `Hs Puerto: ${formatNumber(f.wavePort)} m`,
            `Hs PdE: ${formatNumber(f.wavePde)} m`,
            `Tp PdE: ${formatNumber(f.tpPde)} s`,
            `Di PdE: ${formatNumber(f.dirPde, 0)}°`,
            `Hs Copernicus: ${formatNumber(f.waveCopernicus)} m`,
            `Hs Obs: ${formatNumber(f.waveObs)} m`
          ];
        }
      }
    },
    verticalCursorPlugin: {
      selectedIndex: selectedHour
    },
    pdeWaveArrowsPlugin: {
      datasetIndex: 2,
      directions: dirCop,
      arrowYValue: arrowYValue
    }
  },
  scales: {
    x: {
      grid: { color: "#eef2f7" }
    },
    y: {
   beginAtZero: true,
  max: yMaxChart,
  title: {
    display: true,
    text: "Hs (m)"
  },
  grid: { color: "#e5e7eb" },
}
  }
},
    plugins: [verticalCursorPlugin, pdeWaveArrowsPlugin,daySeparatorPlugin]
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
