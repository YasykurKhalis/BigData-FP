/**
 * LUMBUNG — alert.js
 * Polling alert engine secara berkala dan update banner di halaman beranda.
 */

var ALERT_POLL_INTERVAL_MS = 5 * 60 * 1000;  // 5 menit
var alertPollTimer = null;

/**
 * Ambil data alert terbaru dari API dan update UI jika ada.
 */
function pollAlerts() {
  fetch('/api/alerts')
    .then(function(res) {
      if (!res.ok) throw new Error('API alerts gagal: ' + res.status);
      return res.json();
    })
    .then(function(data) {
      updateAlertBadge(data);
    })
    .catch(function(err) {
      console.warn('[LUMBUNG alert.js]', err.message);
    });
}

/**
 * Update badge jumlah alert di navbar (jika ada elemen #alertBadge).
 */
function updateAlertBadge(data) {
  var badge = document.getElementById('alertBadge');
  if (!badge) return;

  var total = data.total_alerts || 0;
  if (total > 0) {
    badge.textContent = total;
    badge.style.display = 'inline-block';

    // Warna sesuai level tertinggi
    var alerts = data.alerts || [];
    var hasKritis = alerts.some(function(a) { return a.level === 'KRITIS'; });
    var hasSiaga  = alerts.some(function(a) { return a.level === 'SIAGA';  });
    badge.className = 'alert-badge-dot ' + (hasKritis ? 'dot-kritis' : hasSiaga ? 'dot-siaga' : 'dot-waspada');
  } else {
    badge.style.display = 'none';
  }
}

/**
 * Tampilkan toast notifikasi di pojok kanan bawah.
 * @param {string} message  Teks pesan
 * @param {string} level    'kritis'|'siaga'|'waspada'|'aman'
 * @param {number} duration Durasi tampil dalam ms (default 6000)
 */
function showToast(message, level, duration) {
  duration = duration || 6000;
  var container = document.getElementById('toastContainer');
  if (!container) {
    container = document.createElement('div');
    container.id = 'toastContainer';
    container.style.cssText = [
      'position:fixed', 'bottom:24px', 'right:24px', 'z-index:9999',
      'display:flex', 'flex-direction:column', 'gap:8px',
      'max-width:360px', 'pointer-events:none'
    ].join(';');
    document.body.appendChild(container);
  }

  var toast = document.createElement('div');
  toast.className = 'toast toast-' + level;
  toast.style.cssText = [
    'background:#fff', 'border-radius:8px', 'padding:12px 16px',
    'box-shadow:0 4px 16px rgba(0,0,0,.15)',
    'font-size:0.88rem', 'line-height:1.4',
    'opacity:0', 'transition:opacity .3s',
    'pointer-events:auto',
    'border-left:4px solid ' + levelColor(level)
  ].join(';');
  toast.textContent = message;

  container.appendChild(toast);

  // Fade in
  requestAnimationFrame(function() {
    requestAnimationFrame(function() { toast.style.opacity = '1'; });
  });

  // Fade out setelah durasi
  setTimeout(function() {
    toast.style.opacity = '0';
    setTimeout(function() {
      if (toast.parentNode) toast.parentNode.removeChild(toast);
    }, 350);
  }, duration);
}

function levelColor(level) {
  var colorMap = {
    kritis:  '#dc2626',
    siaga:   '#d97706',
    waspada: '#ca8a04',
    aman:    '#16a34a',
  };
  return colorMap[level] || '#2563eb';
}

// Mulai polling saat DOM siap
document.addEventListener('DOMContentLoaded', function() {
  // Polling pertama setelah 3 detik
  setTimeout(function() {
    pollAlerts();
    alertPollTimer = setInterval(pollAlerts, ALERT_POLL_INTERVAL_MS);
  }, 3000);
});
