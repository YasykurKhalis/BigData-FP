/**
 * LUMBUNG — map.js
 * Inisialisasi peta Leaflet untuk sentra produksi.
 * Membutuhkan SENTRA_DATA (array of sentra objects) didefinisikan
 * sebelum script ini dijalankan (disisipkan via template peta_sentra.html).
 */

var sentraMap = null;

var RISK_COLORS = {
  KRITIS:  '#dc2626',
  SIAGA:   '#d97706',
  WASPADA: '#ca8a04',
  AMAN:    '#16a34a',
};

var KOMODITAS_ICONS = {
  beras:             '🌾',
  cabai_rawit_merah: '🌶️',
  cabai_keriting:    '🫑',
  bawang_merah:      '🧅',
  bawang_putih:      '🧄',
};

/**
 * Buat Leaflet CircleMarker berwarna sesuai level risiko.
 */
function makeMarker(sentra) {
  var color   = RISK_COLORS[sentra.risk_level]  || '#6b7280';
  var icon    = KOMODITAS_ICONS[sentra.komoditas] || '📍';
  var radius  = Math.max(8, (sentra.bobot || 0.1) * 32);

  var marker = L.circleMarker([sentra.lat, sentra.lon], {
    radius:      radius,
    fillColor:   color,
    color:       '#fff',
    weight:      2,
    opacity:     1,
    fillOpacity: 0.82,
  });

  marker.bindPopup([
    '<div style="min-width:180px">',
    '<strong style="font-size:1rem">' + icon + ' ' + capitalize(sentra.nama) + '</strong>',
    '<br><small>' + sentra.provinsi + '</small>',
    '<hr style="margin:6px 0">',
    '<b>Komoditas:</b> ' + formatKomoditas(sentra.komoditas), '<br>',
    '<b>Bobot produksi:</b> ' + Math.round(sentra.bobot * 100) + '%', '<br>',
    '<b>Status risiko:</b> <span style="color:' + color + ';font-weight:700">' + sentra.risk_level + '</span>',
    '<br>Indeks: ' + (sentra.risk_index || 0).toFixed(1) + '/100',
    '</div>'
  ].join(''));

  return marker;
}

function capitalize(str) {
  return (str || '').replace(/\b\w/g, function(c) { return c.toUpperCase(); });
}

function formatKomoditas(key) {
  var map = {
    beras:             'Beras',
    cabai_rawit_merah: 'Cabai Rawit Merah',
    cabai_keriting:    'Cabai Merah Keriting',
    bawang_merah:      'Bawang Merah',
    bawang_putih:      'Bawang Putih',
  };
  return map[key] || key;
}

/**
 * Inisialisasi peta Leaflet dan tambahkan semua marker sentra.
 */
function initMap() {
  if (!document.getElementById('sentraMap')) return;
  if (typeof L === 'undefined') {
    console.error('[map.js] Leaflet.js belum dimuat.');
    return;
  }

  // Pusat peta di Jawa Tengah
  sentraMap = L.map('sentraMap').setView([-7.0, 110.0], 7);

  // Tile layer OpenStreetMap
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    maxZoom: 18,
  }).addTo(sentraMap);

  var data = (typeof SENTRA_DATA !== 'undefined') ? SENTRA_DATA : [];
  if (!data.length) {
    console.warn('[map.js] SENTRA_DATA kosong.');
    return;
  }

  // Tambahkan marker per sentra
  var layerGroups = {};
  data.forEach(function(sentra) {
    var marker = makeMarker(sentra);
    marker.addTo(sentraMap);
    var key = formatKomoditas(sentra.komoditas);
    if (!layerGroups[key]) layerGroups[key] = [];
    layerGroups[key].push(marker);
  });

  // Legenda risiko
  var legend = L.control({ position: 'bottomright' });
  legend.onAdd = function() {
    var div = L.DomUtil.create('div', 'map-legend');
    div.style.cssText = [
      'background:white', 'padding:10px 14px', 'border-radius:8px',
      'box-shadow:0 2px 8px rgba(0,0,0,.15)', 'font-size:0.82rem',
      'line-height:1.8'
    ].join(';');
    div.innerHTML = '<strong>Level Risiko</strong><br>' + Object.entries(RISK_COLORS)
      .map(function(entry) {
        return '<span style="display:inline-block;width:12px;height:12px;background:' +
          entry[1] + ';border-radius:50%;margin-right:5px;vertical-align:middle"></span>' +
          entry[0];
      }).join('<br>');
    return div;
  };
  legend.addTo(sentraMap);
}

document.addEventListener('DOMContentLoaded', initMap);
