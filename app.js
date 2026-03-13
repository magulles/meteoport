function getColor(status){
  if(status === "Operational") return "green";
  if(status === "Caution") return "orange";
  return "red";
}




const locations = [
  {
    name: "Valencia Port",
    coords: [39.448, -0.316],
    wave: "1.2 m",
    wind: "14 kt NE",
    status: "Operational"
  },
  {
    name: "Sagunto Port",
    coords: [39.641, -0.214],
    wave: "0.9 m",
    wind: "11 kt E",
    status: "Operational"
  },
  {
    name: "Gandia Port",
    coords: [38.995, -0.153],
    wave: "1.5 m",
    wind: "16 kt ENE",
    status: "Caution"
  }
];

const map = L.map("map").setView([39.35, -0.25], 9);

L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  attribution: "&copy; OpenStreetMap &copy; Carto",
  subdomains: "abcd",
  maxZoom: 19
}).addTo(map);

const infoPanel = document.getElementById("info-panel");

function updatePanel(location) {
  infoPanel.innerHTML = `
    <p><strong>Name:</strong> ${location.name}</p>
    <p><strong>Wave:</strong> ${location.wave}</p>
    <p><strong>Wind:</strong> ${location.wind}</p>
    <p><strong>Status:</strong> ${location.status}</p>
  `;
}

locations.forEach((location) => {
const marker = L.circleMarker(location.coords, {
  radius: 8,
  color: getColor(location.status),
  fillColor: getColor(location.status),
  fillOpacity: 0.9
}).addTo(map);
  
  marker.bindPopup(`
    <strong>${location.name}</strong><br>
    Wave: ${location.wave}<br>
    Wind: ${location.wind}<br>
    Status: ${location.status}
  `);

  marker.on("click", () => {
    updatePanel(location);
  });
});
