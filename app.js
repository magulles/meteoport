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
  greenMax: 1.5,
  yellowMax: 2.5,
  orangeMax: 3.5
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
const chartTitle = document.getElementById("chart-title");

// ============================
// MAPA
// ============================

const map = L.map("map").setView([39.5, 0], 5);
window.map = map;

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

  if (hs < THRESHOLDS.greenMax) return "green";
  if (hs < THRESHOLDS.yellowMax) return "yellow";
  if (hs < THRESHOLDS.orangeMax) return "orange";
  return "red";
}

function getHexColorFromHs(hs) {
  if (hs === null || hs === undefined || Number.isNaN(hs)) return "#94a3b8";

  if (hs < THRESHOLDS.greenMax) return "#16a34a";
  if (hs < THRESHOLDS.yellowMax) return "#eab308";
  if (hs < THRESHOLDS.orangeMax) return "#f97316";
  return "#dc2626";
}

function getRouteStatus(hs) {
  if (hs === null || hs === undefined || Number.isNaN(hs)) {
    return {
      label: "Sin datos",
      color: "#64748b"
    };
  }

  if (hs < 1) {
    return {
      label: "Operativo",
      color: "#16a34a"
    };
  }

  if (hs < 2) {
    return {
      label: "Precaución",
      color: "#eab308"
    };
  }

  if (hs < 3) {
    return {
      label: "Restricción",
      color: "#f97316"
    };
  }

  return {
    label: "No recomendable",
    color: "#dc2626"
  };
}

function escapeHtml(text) {
  return String(text ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function isValidNumber(v) {
  return v !== null && v !== undefined && !Number.isNaN(Number(v));
}

function getPointCoords(point) {
  const lat = isValidNumber(point.lat) ? Number(point.lat) : (
    isValidNumber(point.requested_lat) ? Number(point.requested_lat) : null
  );
  const lon = isValidNumber(point.lon) ? Number(point.lon) : (
    isValidNumber(point.requested_lon) ? Number(point.requested_lon) : null
  );

  if (!isValidNumber(lat) || !isValidNumber(lon)) return null;
  return [lat, lon];
}

function getOperationalWave(f) {
  const hasPde = isValidNumber(f?.hs_pde);
  const hasPort = isValidNumber(f?.hs_puerto);
  const hasCop = isValidNumber(f?.hs_cop);

  if (hasPde) {
    return {
      wave: Number(f.hs_pde),
      tp: isValidNumber(f.tp_pde) ? Number(f.tp_pde) : null,
      dir: isValidNumber(f.di_pde) ? Number(f.di_pde) : null,
      source: "PdE"
    };
  }

  if (hasPort) {
    return {
      wave: Number(f.hs_puerto),
      tp: null,
      dir: null,
      source: "Puerto"
    };
  }

  if (hasCop) {
    return {
      wave: Number(f.hs_cop),
      tp: isValidNumber(f.tp_cop) ? Number(f.tp_cop) : null,
      dir: isValidNumber(f.di_cop) ? Number(f.di_cop) : null,
      source: "Copernicus"
    };
  }

  return {
    wave: null,
    tp: null,
    dir: null,
    source: "Sin datos"
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

      wavePde: isValidNumber(f.hs_pde) ? Number(f.hs_pde) : null,
      tpPde: isValidNumber(f.tp_pde) ? Number(f.tp_pde) : null,
      dirPde: isValidNumber(f.di_pde) ? Number(f.di_pde) : null,

      waveCopernicus: isValidNumber(f.hs_cop) ? Number(f.hs_cop) : null,
      tpCopernicus: isValidNumber(f.tp_cop) ? Number(f.tp_cop) : null,
      dirCopernicus: isValidNumber(f.di_cop) ? Number(f.di_cop) : null,

      wavePort: isValidNumber(f.hs_puerto) ? Number(f.hs_puerto) : null,

      waveObs: isValidNumber(f.hs_obs) ? Number(f.hs_obs) : null,
      windObs: isValidNumber(f.wspeed_obs) ? Number(f.wspeed_obs) : null,

      windSpeed: isValidNumber(f.wspeed_mod) ? Number(f.wspeed_mod) : null,
      windDir: isValidNumber(f.wsdir_mod) ? Number(f.wsdir_mod) : null
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
      .filter(p => p.loc && Array.isArray(p.loc.coords))
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

      if (!isValidNumber(f.wave)) return;

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
    const latlngs = route.locations
      .map(loc => loc.coords)
      .filter(coords => Array.isArray(coords) && coords.length === 2);

    if (latlngs.length < 2) return;

    const polyline = L.polyline(latlngs, {
      color: getRouteDisplayColor(route),
      weight: 3,
      opacity: 0.75,
      pane: "routesPane"
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
  fetch("./meteo_points_merged.json").then(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status} cargando meteo_points_merged.json`);
    return res.json();
  }),
  fetch("./routes.json").then(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status} cargando routes.json`);
    return res.json();
  })
])
  .then(([meteoData, routesData]) => {
    const rawPoints = Array.isArray(meteoData) ? meteoData : (meteoData.points || []);

    locations = rawPoints
      .map(point => {
        const coords = getPointCoords(point);
        if (!coords) return null;

        return {
          pointId: point.point_id,
          name: point.name,
          coords,
          thresholds: { ...THRESHOLDS },
          forecast: buildMergedForecast(point),
          lon: isValidNumber(point.lon) ? Number(point.lon) : null,
          lat: isValidNumber(point.lat) ? Number(point.lat) : null,
          requestedLon: isValidNumber(point.requested_lon) ? Number(point.requested_lon) : null,
          requestedLat: isValidNumber(point.requested_lat) ? Number(point.requested_lat) : null
        };
      })
      .filter(Boolean);

    if (!locations.length) {
      throw new Error("No hay puntos válidos en meteo_points_merged.json");
    }

    routes = buildRoutes(routesData);

    const maxHour = Math.max(0, getForecastLength() - 1);
    hourSlider.max = maxHour;
    hourSlider.value = selectedHour;

    initMarkers();
    initRoutes();
    updateHourLabel();
    updateInfoPanel();
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

function createObsIcon() {
  return L.divIcon({
    className: "obs-marker-icon",
    html: `
      <div class="obs-marker">
        <div class="obs-dot"></div>
        <div class="obs-stick"></div>
      </div>
    `,
    iconSize: [14, 18],
    iconAnchor: [7, 15],
    popupAnchor: [0, -18]
  });
}

function initMarkers() {
  markers.forEach(({ marker }) => map.removeLayer(marker));
  markers = [];

  locations.forEach(loc => {
    const isObsPoint = loc.name?.toLowerCase().startsWith("boya");

    let marker;

    if (isObsPoint) {
      marker = L.marker(loc.coords, {
        icon: createObsIcon()
      }).addTo(map);
    } else {
      marker = L.circleMarker(loc.coords, {
        radius: 3,
        color: "#1f2937",
        fillColor: "#1f2937",
        fillOpacity: 0.9,
        weight: 1
      }).addTo(map);
    }

    marker.bindTooltip(loc.name, { direction: "top", offset: [0, -6] });

    marker.on("click", () => {
      selectedLocation = loc;
      selectedRoute = null;
      updateRouteStyles();
      updateInfoPanel();
      renderChart();
    });

    markers.push({ marker, loc, isObsPoint });
  });
}

function updateMarkers() {
  markers.forEach(({ marker, isObsPoint }) => {
    const color = "#374151";

    if (!isObsPoint && marker.setStyle) {
      marker.setStyle({ color, fillColor: color });
    }
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

  const status = getRouteStatus(f.wave);

  infoPanel.innerHTML = `
    <h3>${escapeHtml(selectedLocation.name)}</h3>
    <p><strong>Time:</strong> ${formatTimeLabel(f.time)}</p>
    <p><strong>Hs:</strong> ${formatNumber(f.wave)} m (${escapeHtml(f.waveSource)})</p>
    <p><strong>Tp:</strong> ${formatNumber(f.tp)} s</p>
    <p><strong>Wave direction:</strong> ${formatNumber(f.dir)}°</p>
    <p><strong>Wind model:</strong> ${formatNumber(f.windSpeed)} m/s</p>
    <p><strong>Wind dir model:</strong> ${formatNumber(f.windDir)}°</p>
    <p><strong>Hs obs:</strong> ${formatNumber(f.waveObs)} m</p>
    <p><strong>Wind obs:</strong> ${formatNumber(f.windObs)} m/s</p>
    <p><strong>Status:</strong> <span style="color:${status.color}; font-weight:700;">${escapeHtml(status.label)}</span></p>
  `;
}

function renderRouteInfoPanel() {
  if (!selectedRoute) return;

  const summary = calculateRouteSummary(selectedRoute);
  const pointsLabel = selectedRoute.resolvedPoints.map(p => p.name).join(" → ");

  if (!summary.hasData) {
    infoPanel.innerHTML = `
      <h3>${escapeHtml(selectedRoute.name)}</h3>
      <p><strong>Salida:</strong> ${formatDateTimeLong(selectedRoute.departure_time)}</p>
      <p><strong>Llegada:</strong> ${formatDateTimeLong(selectedRoute.arrival_time)}</p>
      <p><strong>Puntos:</strong> ${escapeHtml(pointsLabel)}</p>
      <hr style="margin:10px 0;">
      <p>${escapeHtml(summary.reason)}</p>
    `;
    return;
  }

  const status = getRouteStatus(summary.wave);

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
    <p><strong>Estado:</strong> <span style="color:${status.color}; font-weight:700;">${escapeHtml(status.label)}</span></p>
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

    const refForecast = chart.config.options?.plugins?.daySeparatorPlugin?.forecast || [];
    if (selectedIndex < 0 || selectedIndex >= refForecast.length) return;

    const selectedTime = refForecast[selectedIndex]?.time;
    if (!selectedTime) return;

    const x = xScale.getPixelForValue(selectedTime);
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
// CHART PLUGIN: WAVE ARROWS
// ============================

const pdeWaveArrowsPlugin = {
  id: "pdeWaveArrowsPlugin",
  afterDatasetsDraw(chart, args, options) {
    const datasetIndex = options?.datasetIndex ?? 2;
    const directions = options?.directions ?? [];
    const topPaddingPx = options?.topPaddingPx ?? 10;
    const arrowLengthPx = options?.arrowLengthPx ?? 14;
    const arrowHeadPx = options?.arrowHeadPx ?? 5;
    const lineWidth = options?.lineWidth ?? 1.4;
    const color = options?.color ?? "#4b5563";
    const minPixelGap = options?.minPixelGap ?? 18;

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

    const yFixed = chartArea.top + topPaddingPx;
    let lastDrawnX = null;

    meta.data.forEach((pointEl, i) => {
      const hsValue = dataset.data?.[i]?.y ?? null;
      const dirFrom = directions[i];

      if (hsValue === null || hsValue === undefined || Number.isNaN(hsValue)) return;
      if (dirFrom === null || dirFrom === undefined || Number.isNaN(dirFrom)) return;

      const x = pointEl.x;
      const y = yFixed;

      if (lastDrawnX !== null && Math.abs(x - lastDrawnX) < minPixelGap) return;
      lastDrawnX = x;

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
   if (chartTitle) {
    chartTitle.textContent = selectedLocation?.name
      ? selectedLocation.name
      : "Marine forecast";
  }

  const forecast = selectedLocation.forecast;
const firstTimeMs = forecast.length ? new Date(forecast[0].time).getTime() : null;

let xAxisMin = null;
let xAxisMax = null;

if (firstTimeMs !== null) {
  const firstDate = new Date(firstTimeMs);

  xAxisMin = Date.UTC(
    firstDate.getUTCFullYear(),
    firstDate.getUTCMonth(),
    firstDate.getUTCDate(),
    0, 0, 0, 0
  );

  // desde ayer 00h hasta hoy+4d 21h
  // es decir: primer día + 5 días, a las 21:00
  xAxisMax = Date.UTC(
    firstDate.getUTCFullYear(),
    firstDate.getUTCMonth(),
    firstDate.getUTCDate() + 5,
    21, 0, 0, 0
  );
}

  
  const hsPort = forecast.map(f => ({ x: f.time, y: f.wavePort }));
  const hsPde = forecast.map(f => ({ x: f.time, y: f.wavePde }));
  const hsCop = forecast.map(f => ({ x: f.time, y: f.waveCopernicus }));
  const hsObs = forecast.map(f => ({ x: f.time, y: f.waveObs }));

  const dirPde = forecast.map(f => f.dirPde);
  const dirCop = forecast.map(f => f.dirCopernicus);

  if (waveChart) {
    waveChart.destroy();
  }

 const daySeparatorPlugin = {
  id: "daySeparatorPlugin",
  afterDraw(chart) {
    const { ctx, chartArea, scales } = chart;
    const xScale = scales.x;
    const forecast = chart.config.options?.plugins?.daySeparatorPlugin?.forecast || [];

    if (!xScale || !forecast.length) return;

    const dayGroups = [];
    let currentGroup = null;

    for (let i = 0; i < forecast.length; i++) {
      const t = forecast[i]?.time;
      if (!t) continue;

      const d = new Date(t);
      const dayKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}-${d.getUTCDate()}`;

      if (!currentGroup || currentGroup.dayKey !== dayKey) {
        currentGroup = {
          dayKey,
          startIndex: i,
          endIndex: i,
          date: new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()))
        };
        dayGroups.push(currentGroup);
      } else {
        currentGroup.endIndex = i;
      }
    }

    if (!dayGroups.length) return;

    const xMin = xScale.min;
    const xMax = xScale.max;
    if (xMin == null || xMax == null) return;

    const todayRef = new Date();
    const todayUtc = new Date(Date.UTC(
      todayRef.getUTCFullYear(),
      todayRef.getUTCMonth(),
      todayRef.getUTCDate()
    ));

    function diffDays(dateA, dateB) {
      return Math.round((dateA - dateB) / 86400000);
    }

    function getDayLabel(dayDate) {
      const d = diffDays(dayDate, todayUtc);
      if (d === -1) return "ayer";
      if (d === 0) return "hoy";
      if (d > 0) return `+${d}d`;
      return `${d}d`;
    }

    function clamp(val, min, max) {
      return Math.max(min, Math.min(max, val));
    }

    ctx.save();

    // ============================
    // 0) SOMBREADO DE HOY
    // ============================

    const startToday = Date.UTC(
      todayRef.getUTCFullYear(),
      todayRef.getUTCMonth(),
      todayRef.getUTCDate(),
      0, 0, 0, 0
    );

    const startTomorrow = Date.UTC(
      todayRef.getUTCFullYear(),
      todayRef.getUTCMonth(),
      todayRef.getUTCDate() + 1,
      0, 0, 0, 0
    );

    const shadeStart = Math.max(startToday, xMin);
    const shadeEnd = Math.min(startTomorrow, xMax);

    if (shadeEnd > shadeStart) {
      const leftEdge = xScale.getPixelForValue(shadeStart);
      const rightEdge = xScale.getPixelForValue(shadeEnd);

      ctx.fillStyle = "rgba(37, 99, 235, 0.05)";
      ctx.fillRect(
        leftEdge,
        chartArea.top,
        rightEdge - leftEdge,
        chartArea.bottom - chartArea.top
      );
    }

    // ============================
    // 1) SEPARADORES ENTRE DÍAS
    // ============================

    ctx.strokeStyle = "rgba(70, 70, 70, 0.22)";
    ctx.lineWidth = 1.1;
    ctx.setLineDash([4, 4]);

    for (let i = 1; i < dayGroups.length; i++) {
      const boundaryTime = dayGroups[i].date.getTime();

      if (boundaryTime < xMin || boundaryTime > xMax) continue;

      const x = xScale.getPixelForValue(boundaryTime);
      ctx.beginPath();
      ctx.moveTo(x, chartArea.top);
      ctx.lineTo(x, chartArea.bottom);
      ctx.stroke();
    }

    // ============================
    // 2) ETIQUETAS ABAJO
    // ============================

    ctx.setLineDash([]);
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    ctx.font = "600 11px sans-serif";

    dayGroups.forEach((group, idx) => {
      const startMs = Math.max(group.date.getTime(), xMin);

      const nextGroup = dayGroups[idx + 1];
      const naturalEndMs = nextGroup
        ? nextGroup.date.getTime()
        : (new Date(group.date.getTime() + 24 * 3600 * 1000)).getTime();

      const endMs = Math.min(naturalEndMs, xMax);

      if (endMs <= startMs) return;

      const x1 = xScale.getPixelForValue(startMs);
      const x2 = xScale.getPixelForValue(endMs);
      const xMid = (x1 + x2) / 2;

      const label = getDayLabel(group.date);

      ctx.fillStyle = label === "hoy" ? "#111827" : "#6b7280";
      ctx.fillText(
        label,
        clamp(xMid, chartArea.left + 18, chartArea.right - 18),
        chartArea.bottom + 6
      );
    });

    ctx.restore();
  }
};

  const allHs = [
    ...hsPort.map(p => p.y),
    ...hsPde.map(p => p.y),
    ...hsCop.map(p => p.y),
    ...hsObs.map(p => p.y)
  ].filter(v => v != null && !Number.isNaN(v));

  const maxHs = allHs.length ? Math.max(...allHs) : 2;
  const yMaxChart = maxHs + 0.8;

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
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
          borderColor: "#dc2626",
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
          spanGaps: true,
          order: -10
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
      layout: {
        padding: {
          top: 20,
          bottom: 28
        }
      },
      plugins: {
        daySeparatorPlugin: {
          forecast
        },
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
                `Tp Copernicus: ${formatNumber(f.tpCopernicus)} s`,
                `Di Copernicus: ${formatNumber(f.dirCopernicus, 0)}°`,
                `Hs Obs: ${formatNumber(f.waveObs)} m`,
                `Wind obs: ${formatNumber(f.windObs)} m/s`,
                `Wind model: ${formatNumber(f.windSpeed)} m/s`,
                `Wind dir model: ${formatNumber(f.windDir, 0)}°`
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
          topPaddingPx: 18,
          arrowLengthPx: 12,
          arrowHeadPx: 4,
          lineWidth: 1.1,
          minPixelGap: 18
        }
      },
      scales: {
        x: {
  type: "time",
min: xAxisMin,
max: xAxisMax,
  time: {
    unit: "hour",
    stepSize: 3,
    round: "hour",
    tooltipFormat: "yyyy-MM-dd HH:mm",
    displayFormats: {
      hour: "dd-MMM-HH'h'"
    }
  },
  ticks: {
    source: "auto",
    stepSize: 3,
    maxRotation: 55,
    minRotation: 55,
    autoSkip: false
  },
  grid: { color: "#eef2f7" }
},
        y: {
          beginAtZero: true,
          max: yMaxChart,
          title: {
            display: true,
            text: "Hs (m)"
          },
          grid: { color: "#e5e7eb" }
        }
      }
    },
    plugins: [verticalCursorPlugin, pdeWaveArrowsPlugin, daySeparatorPlugin]
  });

  window.chart = waveChart;
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
  updateRouteStyles();
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
