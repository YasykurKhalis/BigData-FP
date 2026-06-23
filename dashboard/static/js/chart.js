/**
 * LUMBUNG — chart.js
 * Rendering grafik prediksi harga menggunakan Chart.js (CDN).
 * Dipanggil dari template komoditas.html setelah DOM ready.
 */

// Simpan referensi chart yang sudah dirender agar bisa di-destroy saat tab switch
var chartInstances = {};

/**
 * Render grafik prediksi harga untuk komoditas tertentu.
 * @param {string} komId - ID komoditas (misal: "beras")
 */
function renderForecastChart(komId) {
  var dataEl = document.querySelector('[data-chart="' + komId + '"]');
  if (!dataEl) return;

  var canvasEl = document.getElementById('chart-' + komId);
  if (!canvasEl) return;

  // Jika chart sudah ada, destroy dulu
  if (chartInstances[komId]) {
    chartInstances[komId].destroy();
  }

  var labels   = JSON.parse(dataEl.dataset.labels  || '[]');
  var values   = JSON.parse(dataEl.dataset.values  || '[]');
  var current  = parseFloat(dataEl.dataset.current || '0');
  var color    = dataEl.dataset.color || 'rgba(37,99,235,0.7)';

  if (!labels.length || !values.length) return;

  // Gabungkan "Hari Ini" sebagai titik awal
  var allLabels = ['Hari Ini'].concat(labels);
  var allValues = [current].concat(values);

  // Perlu Chart.js tersedia — cek dulu
  if (typeof Chart === 'undefined') {
    loadChartJs(function() {
      _doRender(komId, canvasEl, allLabels, allValues, current, color);
    });
  } else {
    _doRender(komId, canvasEl, allLabels, allValues, current, color);
  }
}

function _doRender(komId, canvasEl, labels, values, current, color) {
  var ctx = canvasEl.getContext('2d');

  // Buat gradient
  var gradient = ctx.createLinearGradient(0, 0, 0, canvasEl.height);
  gradient.addColorStop(0, color.replace('0.7', '0.15'));
  gradient.addColorStop(1, color.replace('0.7', '0.02'));

  chartInstances[komId] = new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: 'Prediksi Harga (Rp/kg)',
        data: values,
        borderColor: color,
        backgroundColor: gradient,
        borderWidth: 2.5,
        fill: true,
        tension: 0.35,
        pointRadius: 4,
        pointHoverRadius: 6,
        pointBackgroundColor: color,
      }]
    },
    options: {
      responsive: true,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              return ' Rp' + Number(ctx.raw).toLocaleString('id-ID') + '/kg';
            }
          }
        }
      },
      scales: {
        y: {
          ticks: {
            callback: function(val) {
              return 'Rp' + Number(val).toLocaleString('id-ID');
            }
          },
          grid: { color: 'rgba(0,0,0,.05)' }
        },
        x: {
          grid: { display: false }
        }
      }
    }
  });
}

/**
 * Load Chart.js dari CDN secara dinamis jika belum tersedia.
 */
function loadChartJs(callback) {
  var script = document.createElement('script');
  script.src = 'https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js';
  script.onload = callback;
  document.head.appendChild(script);
}

// Inisialisasi saat DOM siap
document.addEventListener('DOMContentLoaded', function() {
  // Otomatis render semua chart yang ada di halaman
  var allChartData = document.querySelectorAll('[data-chart]');
  if (allChartData.length === 0) return;

  function initCharts() {
    allChartData.forEach(function(el) {
      var parent = document.getElementById('tab-' + el.dataset.chart);
      // Hanya render jika tab-nya visible (tidak hidden)
      if (parent && !parent.classList.contains('hidden')) {
        renderForecastChart(el.dataset.chart);
      }
    });
  }

  if (typeof Chart === 'undefined') {
    loadChartJs(initCharts);
  } else {
    initCharts();
  }
});
