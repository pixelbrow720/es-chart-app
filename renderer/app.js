/* ============================================================
   ES·TBBO Suite — app.js  v2.0
   Improvements:
     - FIX: Bar type toggle correctly updates state
     - FIX: Date range validated before backtest
     - FIX: Equity chart tooltip uses correct date format
     - ADD: Chart tab with candlestick + volume + delta
     - ADD: Template modal with 5 ready-made strategies
     - ADD: CSV export for trades
     - ADD: Ctrl+Enter keyboard shortcut to run backtest
     - ADD: Color-coded console output
     - ADD: Trade log filter + column sort
     - ADD: Quick stats badges in backtest toolbar
     - ADD: Session filter propagated to all API calls
     - ADD: Chart auto-refresh when tab is switched
     - ADD: Loading overlay for long operations
   ============================================================ */

const API = 'http://127.0.0.1:8765'

let state = {
  filePath:    null,
  symbol:      null,
  barType:     'time',
  interval:    '5min',
  threshold:   1000,
  dateFrom:    null,
  dateTo:      null,
  session:     'ALL',
  btResult:    null,
  chartBars:   null,
  tradesAll:   [],     // unfiltered trade rows for filtering
  sortCol:     null,
  sortDir:     1,
}

let equityChart, ddChart, mcPathsChart, mcDistChart, mcDdChart, pnlDistChart
let mainChart   // candlestick chart
let editor

// ── Helpers ───────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id)
const enc = s => encodeURIComponent(s)
const fmtDollar = (v, maxDec = 2) => {
  if (v == null) return '—'
  const abs = Math.abs(v)
  const s   = abs.toLocaleString(undefined, { maximumFractionDigits: maxDec })
  return (v < 0 ? '-$' : '$') + s
}
const fmtDt = ts => {
  const d = new Date(ts * 1000)
  return d.toLocaleDateString() + ' ' +
    String(d.getHours()).padStart(2,'0') + ':' +
    String(d.getMinutes()).padStart(2,'0')
}
const fmtDuration = secs => {
  if (!secs) return '—'
  if (secs < 60)   return secs + 's'
  if (secs < 3600) return Math.floor(secs / 60) + 'm'
  return (secs / 3600).toFixed(1) + 'h'
}

function setStatus(msg, cls = '') {
  const el = $('status')
  el.textContent = msg
  el.className   = cls
}

function setBtStatus(msg, cls = '') {
  const el = $('bt-status')
  el.textContent = msg
  el.className   = cls
}

function showLoading(msg = 'Running…') {
  const el = $('loading-overlay')
  $('loading-msg').textContent = msg
  el.classList.add('active')
}

function hideLoading() {
  $('loading-overlay').classList.remove('active')
}

// ── Colored console ───────────────────────────────────────────────────────
function log(msg, type = 'auto') {
  const el = $('strat-console')
  const ts = new Date().toLocaleTimeString()

  let cls = 'log-plain'
  if (type === 'auto') {
    if (msg.startsWith('✅') || msg.startsWith('✓')) cls = 'log-ok'
    else if (msg.startsWith('❌') || msg.startsWith('✖')) cls = 'log-err'
    else if (msg.startsWith('⚠') || msg.startsWith('⏳')) cls = 'log-warn'
    else if (msg.startsWith('📋') || msg.startsWith('📂') || msg.startsWith('💾') || msg.startsWith('📊')) cls = 'log-info'
    else if (msg.startsWith('[')) cls = 'log-plain'
    else cls = 'log-info'
  } else {
    const map = { ok: 'log-ok', err: 'log-err', warn: 'log-warn', info: 'log-info' }
    cls = map[type] || 'log-plain'
  }

  const line = document.createElement('span')
  line.className = cls
  line.textContent = (el.textContent ? '\n' : '') + `[${ts}] ${msg}`
  el.appendChild(line)
  el.scrollTop = el.scrollHeight
}

// ── INIT ──────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', async () => {
  // Init echarts
  equityChart  = echarts.init($('equity-chart'))
  ddChart      = echarts.init($('dd-chart'))
  mcPathsChart = echarts.init($('mc-paths-chart'))
  mcDistChart  = echarts.init($('mc-dist-chart'))
  mcDdChart    = echarts.init($('mc-dd-chart'))
  pnlDistChart = echarts.init($('pnl-dist-chart'))
  mainChart    = echarts.init($('main-chart'))

  // Sync crosshair between equity + drawdown
  equityChart.on('updateAxisPointer', evt => {
    if (evt.axesInfo?.length) {
      ddChart.dispatchAction({ type: 'updateAxisPointer', currTrigger: 'none' })
    }
  })

  // Init CodeMirror
  editor = CodeMirror.fromTextArea($('code-editor'), {
    mode: 'python', theme: 'dracula', lineNumbers: true,
    indentUnit: 4, tabSize: 4, indentWithTabs: false,
    lineWrapping: false, autofocus: true,
    extraKeys: {
      Tab: cm => cm.replaceSelection('    '),
      'Ctrl-Enter': () => runBacktest(),
      'Ctrl-S': () => $('save-btn').click(),
    },
  })
  editor.setValue(TEMPLATES[0].code)
  editor.setSize('100%', '100%')

  window.addEventListener('resize', resizeAll)
  setTimeout(resizeAll, 300)

  await scanDatasetFolder()
})

function resizeAll() {
  [equityChart, ddChart, mcPathsChart, mcDistChart, mcDdChart, pnlDistChart, mainChart]
    .forEach(c => c && c.resize())
}

// ── TABS ──────────────────────────────────────────────────────────────────
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'))
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'))
    tab.classList.add('active')
    $(`${tab.dataset.tab}-page`).classList.add('active')
    setTimeout(resizeAll, 120)

    // Auto-load chart when switching to chart tab
    if (tab.dataset.tab === 'chart' && state.filePath && !state.chartBars) {
      loadChartData()
    }
  })
})

// ── DATASET AUTO-SCAN ─────────────────────────────────────────────────────
async function scanDatasetFolder() {
  setStatus('scanning dataset/…', 'load')
  try {
    const files = await window.electronAPI.listDatasetFiles()
    const sel   = $('dataset-select')

    if (files.length === 0) {
      sel.innerHTML = '<option value="">— no .parquet files in dataset/ —</option>'
      setStatus('no files found in dataset/', 'err')
      return
    }

    sel.innerHTML = files.map(f =>
      `<option value="${f.path}">${f.name}</option>`
    ).join('')

    await loadFile(files[0].path)
  } catch (e) {
    console.error('Dataset scan error:', e)
    $('dataset-select').innerHTML = '<option value="">— error scanning dataset/ —</option>'
    setStatus('error scanning dataset folder', 'err')
  }
}

$('dataset-select').addEventListener('change', async () => {
  const p = $('dataset-select').value
  if (p) await loadFile(p)
})

// ── OPEN FILE (browse) ────────────────────────────────────────────────────
$('file-btn').addEventListener('click', async () => {
  const p = await window.electronAPI.openFileDialog()
  if (!p) return
  const sel = $('dataset-select')
  const exists = [...sel.options].some(o => o.value === p)
  if (!exists) {
    const name = p.split(/[\\/]/).pop()
    sel.appendChild(new Option(name, p))
  }
  sel.value = p
  await loadFile(p)
})

// ── LOAD FILE ─────────────────────────────────────────────────────────────
async function loadFile(filePath) {
  setStatus('loading…', 'load')
  try {
    const res = await fetch(`${API}/load?path=${enc(filePath)}`)
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`)
    const info = await res.json()
    state.filePath  = filePath
    state.chartBars = null   // invalidate chart cache

    if (info.first) {
      $('date-from').value = info.first.slice(0, 10)
      $('date-to').value   = (info.last || info.first).slice(0, 10)
      state.dateFrom = $('date-from').value
      state.dateTo   = $('date-to').value
    }

    // Populate symbols
    const sRes = await fetch(`${API}/symbols?path=${enc(filePath)}`)
    if (!sRes.ok) throw new Error(`Symbols HTTP ${sRes.status}`)
    const syms = await sRes.json()

    $('sym-select').innerHTML = syms.map(s =>
      `<option value="${s.symbol}">${s.symbol} (${s.count.toLocaleString()})</option>`
    ).join('')
    state.symbol = syms[0]?.symbol

    $('fb-rows').textContent  = info.rows.toLocaleString() + ' rows'
    $('fb-range').textContent = (info.first?.slice(0,10) || '?') + ' → ' + (info.last?.slice(0,10) || '?')
    $('fb-bars').textContent  = '— bars'

    $('run-strat-btn').disabled = false
    $('bt-run-btn').disabled    = false

    const fname = filePath.split(/[\\/]/).pop()
    setStatus(`ready  •  ${info.rows.toLocaleString()} rows`, 'ok')
    log(`✅ Loaded: ${fname}\n   Rows   : ${info.rows.toLocaleString()}\n   Range  : ${info.first?.slice(0,10)} → ${info.last?.slice(0,10)}\n   Symbols: ${syms.map(s => s.symbol).join(', ')}`)
  } catch (e) {
    setStatus('error loading file', 'err')
    log(`❌ Load error: ${e.message}`)
  }
}

// ── BAR TYPE TOGGLE ───────────────────────────────────────────────────────
document.querySelectorAll('.bbt').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.bbt').forEach(b => b.classList.remove('active'))
    btn.classList.add('active')
    state.barType  = btn.dataset.type
    state.chartBars = null   // invalidate chart cache

    const isTime = state.barType === 'time'
    $('time-ctrl').style.display   = isTime ? 'flex' : 'none'
    $('thresh-ctrl').style.display = isTime ? 'none' : 'flex'

    const labels   = { volume: 'Volume / bar', tick: 'Ticks / bar', range: 'Points / bar' }
    const defaults = { volume: 1000, tick: 500, range: 4 }
    $('thresh-lbl').textContent = labels[state.barType] || 'N'
    $('thresh-inp').value       = defaults[state.barType] || 1000
    state.threshold = defaults[state.barType] || 1000
  })
})

$('tf-select').addEventListener('change', () => {
  state.interval  = $('tf-select').value
  state.chartBars = null
})

$('thresh-inp').addEventListener('change', () => {
  state.threshold = parseFloat($('thresh-inp').value) || 1000
  state.chartBars = null
})

$('date-from').addEventListener('change', () => { state.dateFrom = $('date-from').value; state.chartBars = null })
$('date-to').addEventListener('change',   () => { state.dateTo   = $('date-to').value;   state.chartBars = null })
$('session-select').addEventListener('change', () => {
  state.session   = $('session-select').value
  state.chartBars = null
})
$('sym-select').addEventListener('change', () => {
  state.symbol    = $('sym-select').value
  state.chartBars = null
})

// ── STRATEGY EDITOR ACTIONS ───────────────────────────────────────────────
$('save-btn').addEventListener('click', () => {
  const code = editor.getValue()
  const blob = new Blob([code], { type: 'text/plain' })
  const a    = document.createElement('a')
  a.href     = URL.createObjectURL(blob)
  a.download = 'strategy.py'
  a.click()
  log('💾 Saved as strategy.py')
})

$('load-strat-btn').addEventListener('click', () => {
  const input  = document.createElement('input')
  input.type   = 'file'
  input.accept = '.py,.txt'
  input.onchange = e => {
    const file = e.target.files[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = ev => {
      editor.setValue(ev.target.result)
      log(`📂 Loaded strategy: ${file.name}`)
    }
    reader.readAsText(file)
  }
  input.click()
})

$('clear-console-btn').addEventListener('click', () => {
  $('strat-console').innerHTML = ''
})

$('copy-console-btn').addEventListener('click', () => {
  navigator.clipboard.writeText($('strat-console').textContent)
    .then(() => log('📋 Console copied to clipboard'))
})

// ── TEMPLATE MODAL ────────────────────────────────────────────────────────
$('tpl-btn').addEventListener('click', openTemplateModal)
$('tpl-modal-close').addEventListener('click', closeTemplateModal)
$('tpl-modal').addEventListener('click', e => {
  if (e.target === $('tpl-modal')) closeTemplateModal()
})

function openTemplateModal() {
  const list = $('tpl-list')
  list.innerHTML = ''
  TEMPLATES.forEach((tpl, i) => {
    const card = document.createElement('div')
    card.className = 'tpl-card'
    card.innerHTML = `
      <div class="tpl-card-title">${tpl.name}</div>
      <div class="tpl-card-tags">${tpl.tags.map(t => `<span class="tpl-tag ${t.cls}">${t.label}</span>`).join('')}</div>
      <div class="tpl-card-desc">${tpl.desc}</div>
    `
    card.addEventListener('click', () => {
      editor.setValue(tpl.code)
      log(`📋 Template loaded: ${tpl.name}`)
      closeTemplateModal()
    })
    list.appendChild(card)
  })
  $('tpl-modal').style.display = 'flex'
}

function closeTemplateModal() {
  $('tpl-modal').style.display = 'none'
}

// ── CHART TAB ─────────────────────────────────────────────────────────────
$('ch-refresh-btn').addEventListener('click', () => {
  state.chartBars = null
  loadChartData()
})

// Overlay & panel toggles
document.querySelectorAll('.ch-overlay, .ch-panel').forEach(cb => {
  cb.addEventListener('change', () => {
    if (state.chartBars) renderChart(state.chartBars)
  })
})

async function loadChartData() {
  if (!state.filePath) return
  setStatus('loading bars…', 'load')

  try {
    const params = new URLSearchParams({
      path:      state.filePath,
      symbol:    $('sym-select').value || state.symbol || '',
      bar_type:  state.barType,
      interval:  state.interval,
      threshold: state.threshold,
      session:   state.session,
    })
    if (state.dateFrom) params.set('date_from', state.dateFrom)
    if (state.dateTo)   params.set('date_to',   state.dateTo)

    const res  = await fetch(`${API}/bars?${params}`)
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const bars = await res.json()

    state.chartBars = bars
    $('ch-bar-count').textContent = bars.length.toLocaleString() + ' bars'
    $('fb-bars').textContent = bars.length.toLocaleString() + ' bars'
    renderChart(bars)
    setStatus(`chart: ${bars.length.toLocaleString()} bars`, 'ok')
  } catch (e) {
    setStatus('chart load error', 'err')
    log(`❌ Chart load error: ${e.message}`)
  }
}

function calcEMA(values, period) {
  const k   = 2 / (period + 1)
  const ema = []
  let prev  = null
  for (const v of values) {
    if (prev === null) { prev = v; ema.push(v); continue }
    prev = v * k + prev * (1 - k)
    ema.push(Math.round(prev * 100) / 100)
  }
  return ema
}

function renderChart(bars) {
  if (!bars || bars.length === 0) return

  const showVwap     = $('ch-vwap').checked
  const showEma9     = $('ch-ema9').checked
  const showEma21    = $('ch-ema21').checked
  const showVol      = $('ch-vol-panel').checked
  const showDelta    = $('ch-delta-panel').checked
  const showCumDelta = $('ch-cumdelta-panel').checked

  const times   = bars.map(b => new Date(b.time * 1000).toISOString())
  const candles = bars.map(b => [b.open, b.close, b.low, b.high])
  const closes  = bars.map(b => b.close)
  const buyVol  = bars.map(b => b.buy_vol  || 0)
  const sellVol = bars.map(b => b.sell_vol || 0)
  const deltas  = bars.map(b => b.delta    || 0)
  const cumDels = bars.map(b => b.cum_delta || 0)
  const vwaps   = bars.map(b => b.vwap)
  const ema9    = calcEMA(closes, 9)
  const ema21   = calcEMA(closes, 21)

  // Determine active panels
  const activePanels = []
  if (showVol)      activePanels.push('vol')
  if (showDelta)    activePanels.push('delta')
  if (showCumDelta) activePanels.push('cumdelta')

  // Grid layout
  const candleBottom = activePanels.length === 0 ? '8%'
    : activePanels.length === 1 ? '26%'
    : activePanels.length === 2 ? '40%'
    : '52%'

  const panelH = 12   // % height per panel
  const grids  = [{ left: 80, right: 16, top: 10, bottom: candleBottom }]
  const xAxes  = []
  const yAxes  = []
  const series = []
  const dzXIndexes = [0]

  let panelIdx = 0
  activePanels.forEach((name, i) => {
    const isLast  = i === activePanels.length - 1
    const bottom  = isLast ? '4%' : `${4 + (activePanels.length - 1 - i) * (panelH + 2)}%`
    const top     = `${100 - parseFloat(candleBottom) - (i + 1) * (panelH + 2) + 2}%`
    grids.push({ left: 80, right: 16, top, bottom, height: panelH + '%' })
    dzXIndexes.push(i + 1)
  })

  // X axis definitions (one per grid)
  for (let i = 0; i <= activePanels.length; i++) {
    xAxes.push({
      type: 'category',
      data: times,
      gridIndex: i,
      axisLabel: {
        show: i === activePanels.length || activePanels.length === 0,
        color: '#505090', fontSize: 9, fontFamily: 'JetBrains Mono',
        formatter: v => {
          const d = new Date(v)
          return `${d.getMonth()+1}/${d.getDate()}`
        },
      },
      axisLine: { lineStyle: { color: '#202045' } },
      splitLine: { show: false },
      axisTick: { show: false },
    })
  }

  // Y axis for candlestick
  yAxes.push({
    type: 'value', scale: true, gridIndex: 0,
    axisLabel: {
      color: '#505090', fontSize: 9, fontFamily: 'JetBrains Mono',
      formatter: v => v.toFixed(2),
    },
    splitLine: { lineStyle: { color: '#111130', type: 'dashed' } },
  })

  // Candlestick series
  series.push({
    name: 'Candlestick', type: 'candlestick',
    xAxisIndex: 0, yAxisIndex: 0,
    data: candles,
    itemStyle: {
      color: 'rgba(134,112,255,.9)',        // bull fill (close > open)
      color0: 'rgba(255,0,149,.9)',          // bear fill
      borderColor: '#8670ff',
      borderColor0: '#ff0095',
    },
  })

  // VWAP overlay
  if (showVwap) {
    series.push({
      name: 'VWAP', type: 'line',
      xAxisIndex: 0, yAxisIndex: 0,
      data: vwaps, smooth: false, symbol: 'none',
      lineStyle: { color: '#f59e0b', width: 1.2, type: 'dashed' },
    })
  }
  if (showEma9) {
    series.push({
      name: 'EMA9', type: 'line',
      xAxisIndex: 0, yAxisIndex: 0,
      data: ema9, smooth: false, symbol: 'none',
      lineStyle: { color: '#00e5ff', width: 1, type: 'solid' },
    })
  }
  if (showEma21) {
    series.push({
      name: 'EMA21', type: 'line',
      xAxisIndex: 0, yAxisIndex: 0,
      data: ema21, smooth: false, symbol: 'none',
      lineStyle: { color: '#7c3aed', width: 1, type: 'solid' },
    })
  }

  // Sub-panels
  activePanels.forEach((name, i) => {
    const gi = i + 1  // gridIndex

    yAxes.push({
      type: 'value', scale: true, gridIndex: gi,
      axisLabel: {
        color: '#404070', fontSize: 8, fontFamily: 'JetBrains Mono',
        formatter: v => {
          if (name === 'vol')      return (v/1000).toFixed(0) + 'K'
          if (name === 'delta')    return v > 0 ? '+'+v : String(v)
          if (name === 'cumdelta') return v > 0 ? '+'+v : String(v)
          return v
        },
      },
      splitLine: { lineStyle: { color: '#111130', type: 'dashed' } },
    })

    if (name === 'vol') {
      series.push({
        name: 'Buy Vol', type: 'bar', stack: 'vol',
        xAxisIndex: gi, yAxisIndex: gi,
        data: buyVol.map(v => ({ value: v, itemStyle: { color: 'rgba(134,112,255,.75)' } })),
        barMaxWidth: 6,
      })
      series.push({
        name: 'Sell Vol', type: 'bar', stack: 'vol',
        xAxisIndex: gi, yAxisIndex: gi,
        data: sellVol.map(v => ({ value: -v, itemStyle: { color: 'rgba(255,0,149,.75)' } })),
        barMaxWidth: 6,
      })
    }
    if (name === 'delta') {
      series.push({
        name: 'Delta', type: 'bar',
        xAxisIndex: gi, yAxisIndex: gi,
        data: deltas.map(v => ({
          value: v,
          itemStyle: { color: v >= 0 ? 'rgba(134,112,255,.75)' : 'rgba(255,0,149,.75)' },
        })),
        barMaxWidth: 6,
      })
    }
    if (name === 'cumdelta') {
      series.push({
        name: 'Cum Δ', type: 'line',
        xAxisIndex: gi, yAxisIndex: gi,
        data: cumDels, smooth: false, symbol: 'none',
        lineStyle: { color: '#00e5ff', width: 1.5 },
        areaStyle: {
          color: {
            type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(0,229,255,.18)' },
              { offset: 1, color: 'rgba(0,229,255,.01)' },
            ],
          },
        },
      })
    }
  })

  const opt = {
    animation:       false,
    backgroundColor: 'transparent',
    grid:    grids,
    xAxis:   xAxes,
    yAxis:   yAxes,
    series:  series,
    dataZoom: [
      { type: 'inside', xAxisIndex: dzXIndexes, start: 80, end: 100 },
      {
        type: 'slider', xAxisIndex: dzXIndexes, bottom: 0, height: 14,
        borderColor: '#202045', backgroundColor: '#0d0d1c',
        fillerColor: 'rgba(0,229,255,.07)',
        handleStyle: { color: '#00e5ff' },
        textStyle: { color: '#404070', fontFamily: 'JetBrains Mono', fontSize: 9 },
      },
    ],
    tooltip: {
      trigger: 'axis', axisPointer: { type: 'cross' },
      backgroundColor: '#111122', borderColor: '#252550',
      textStyle: { color: '#dde0ff', fontFamily: 'JetBrains Mono', fontSize: 10 },
      formatter: params => {
        if (!params?.length) return ''
        const idx = params[0]?.dataIndex
        const b   = bars[idx]
        if (!b) return ''
        const dt  = new Date(b.time * 1000)
        const dtStr = dt.toLocaleDateString() + ' ' + dt.toLocaleTimeString()
        const dColor = b.delta >= 0 ? '#8670ff' : '#ff0095'
        let html = `<div style="font-size:9px;color:#555;margin-bottom:4px">${dtStr}</div>`
        html += `<div>O <b style="color:#e4e4f0">${b.open?.toFixed(2)}</b>  H <b style="color:#8670ff">${b.high?.toFixed(2)}</b>  L <b style="color:#ff0095">${b.low?.toFixed(2)}</b>  C <b style="color:#00e5ff">${b.close?.toFixed(2)}</b></div>`
        html += `<div>Vol <b>${(b.volume||0).toFixed(0)}</b>  Δ <b style="color:${dColor}">${b.delta >= 0 ? '+' : ''}${b.delta?.toFixed(0)}</b>  Imb <b>${b.size_imb?.toFixed(3) ?? '—'}</b></div>`
        if (b.vwap) html += `<div>VWAP <b style="color:#f59e0b">${b.vwap?.toFixed(2)}</b></div>`
        return html
      },
    },
    legend: {
      top: 4, right: 16,
      textStyle: { color: '#7070a0', fontSize: 9, fontFamily: 'JetBrains Mono' },
      itemWidth: 14, itemHeight: 2,
    },
  }

  mainChart.setOption(opt, true)
}

// ── RUN BACKTEST ──────────────────────────────────────────────────────────
$('run-strat-btn').addEventListener('click', runBacktest)
$('bt-run-btn').addEventListener('click', runBacktest)

// Global keyboard shortcut
document.addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
    e.preventDefault()
    if (!$('run-strat-btn').disabled) runBacktest()
  }
})

async function runBacktest() {
  if (!state.filePath) { log('❌ No dataset loaded.'); return }
  const code = editor.getValue().trim()
  if (!code) { log('❌ Strategy is empty.'); return }

  // Validate dates
  const df = $('date-from').value
  const dt = $('date-to').value
  if (df && dt && df > dt) {
    log('❌ Date FROM must be before date TO.'); return
  }

  setStatus('running backtest…', 'load')
  setBtStatus('running…', 'load')
  $('run-strat-btn').disabled = true
  $('bt-run-btn').disabled    = true
  $('bt-export-btn').disabled = true
  showLoading('Running backtest…')
  log('⏳ Running backtest…')

  const payload = {
    path:            state.filePath,
    symbol:          $('sym-select').value,
    bar_type:        state.barType,
    interval:        $('tf-select').value,
    threshold:       parseFloat($('thresh-inp').value) || 1000,
    date_from:       df || null,
    date_to:         dt || null,
    session:         $('session-select').value,
    strategy:        editor.getValue(),
    initial_capital: parseFloat($('p-capital').value)    || 100000,
    commission:      parseFloat($('p-commission').value) || 2,
    slippage:        parseFloat($('p-slippage').value)   || 0.25,
    contract_size:   parseFloat($('p-contract').value)   || 50,
  }

  try {
    const res    = await fetch(`${API}/backtest`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    })
    const result = await res.json()

    if (!result.success) {
      log(`❌ BACKTEST ERROR:\n${result.error}`)
      setBtStatus('error — check console', 'err')
      setStatus('backtest error', 'err')
      return
    }

    state.btResult = result
    const s = result.stats

    if (!s.total_trades) {
      log('⚠️  Backtest ran but produced 0 trades. Check your signal logic or date range.')
      setBtStatus('0 trades — adjust strategy or date range', 'err')
      setStatus('0 trades', 'err')
      return
    }

    const barInfo = payload.bar_type === 'time'
      ? `${payload.interval}`
      : `${payload.bar_type.toUpperCase()} thr=${payload.threshold}`

    log(
      `✅ Backtest complete!\n` +
      `   Bars      : ${result.bar_count?.toLocaleString() ?? '?'}  (${barInfo})\n` +
      `   Trades    : ${s.total_trades}  |  Win rate : ${s.win_rate}%\n` +
      `   Total PnL : ${fmtDollar(s.total_pnl)}\n` +
      `   Sharpe    : ${s.sharpe_ratio}  |  Max DD  : ${s.max_drawdown_pct}%\n` +
      `   Calmar    : ${s.calmar_ratio}  |  Sortino : ${s.sortino_ratio}\n` +
      `   Avg Dur   : ${fmtDuration(s.avg_trade_duration)}`
    )

    setBtStatus(
      `${s.total_trades} trades  |  PnL: ${fmtDollar(s.total_pnl)}  |  WR: ${s.win_rate}%  |  Sharpe: ${s.sharpe_ratio}`,
      s.total_pnl >= 0 ? 'ok' : 'err'
    )
    setStatus(`${s.total_trades} trades  •  ${fmtDollar(s.total_pnl)}`, s.total_pnl >= 0 ? 'ok' : 'err')

    renderStats(result.stats)
    renderEquity(result.equity)
    renderTradesTable(result.trades)
    renderAnalytics(result)
    renderQuickBadges(result.stats)
    $('bt-export-btn').disabled = false

    // Auto-switch to Backtest tab
    document.querySelector('[data-tab="backtest"]').click()

  } catch (e) {
    log(`❌ FETCH ERROR: ${e.message}`)
    setBtStatus('fetch error', 'err')
    setStatus('error', 'err')
  } finally {
    $('run-strat-btn').disabled = false
    $('bt-run-btn').disabled    = false
    hideLoading()
  }
}

// ── EXPORT CSV ────────────────────────────────────────────────────────────
$('bt-export-btn').addEventListener('click', async () => {
  if (!state.btResult) return
  try {
    const res = await fetch(`${API}/export/trades`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({
        trades: state.btResult.trades,
        stats:  state.btResult.stats,
      }),
    })
    const blob = await res.blob()
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement('a')
    a.href     = url
    a.download = `trades_${new Date().toISOString().slice(0,10)}.csv`
    a.click()
    URL.revokeObjectURL(url)
    log('💾 Trades exported to CSV')
  } catch (e) {
    log(`❌ Export error: ${e.message}`)
  }
})

// ── QUICK BADGES ──────────────────────────────────────────────────────────
function renderQuickBadges(s) {
  const badges = [
    ['TRADES',   s.total_trades,                    'neu'],
    ['WIN%',     s.win_rate + '%',                  s.win_rate >= 50 ? 'up' : 'dn'],
    ['PNL',      fmtDollar(s.total_pnl),            s.total_pnl >= 0 ? 'up' : 'dn'],
    ['SHARPE',   s.sharpe_ratio,                    s.sharpe_ratio >= 1 ? 'up' : s.sharpe_ratio >= 0 ? 'neu' : 'dn'],
    ['MAX DD',   s.max_drawdown_pct + '%',          'dn'],
  ]
  $('bt-quick-badges').innerHTML = badges.map(([k, v, cls]) =>
    `<div class="qt-badge">
       <span class="qt-badge-k">${k}</span>
       <span class="qt-badge-v ${cls}">${v}</span>
     </div>`
  ).join('')
}

// ── STATS SIDEBAR ─────────────────────────────────────────────────────────
function renderStats(s) {
  $('bt-hint').style.display = 'none'
  const wrap = $('bt-stats-wrap')
  wrap.innerHTML = ''

  const cards = [
    {
      title: '📈 Performance',
      rows: [
        ['Total Return',  'total_return_pct', '%', true],
        ['Total PnL',     'total_pnl',        '$', true],
        ['Final Equity',  'final_equity',      '$', true],
        ['Sharpe Ratio',  'sharpe_ratio',      '',  true],
        ['Sortino Ratio', 'sortino_ratio',     '',  true],
        ['Calmar Ratio',  'calmar_ratio',      '',  true],
      ],
    },
    {
      title: '🔢 Trades',
      rows: [
        ['Total Trades',    'total_trades',      '', null],
        ['Win Rate',        'win_rate',          '%', true],
        ['Avg Win',         'avg_win',           '$', true],
        ['Avg Loss',        'avg_loss',          '$', false],
        ['Best Trade',      'best_trade',        '$', true],
        ['Worst Trade',     'worst_trade',       '$', false],
        ['Avg Duration',    'avg_trade_duration', 's', null],
        ['Max Consec. Win', 'max_consec_wins',   '',  null],
        ['Max Consec. L',  'max_consec_losses', '',  null],
      ],
    },
    {
      title: '🛡 Risk',
      rows: [
        ['Max Drawdown',  'max_drawdown_pct', '%', false],
        ['Profit Factor', 'profit_factor',    '',  true],
        ['RR Ratio',      'rr_ratio',         '',  true],
        ['Expectancy',    'expectancy',       '$',  true],
        ['Avg Trade',     'avg_trade',        '$',  true],
        ['Gross Profit',  'gross_profit',     '$',  true],
        ['Gross Loss',    'gross_loss',       '$',  false],
      ],
    },
  ]

  cards.forEach(card => {
    const div = document.createElement('div')
    div.className = 'stat-card'
    div.innerHTML = `<div class="stat-card-title">${card.title}</div>`

    card.rows.forEach(([key, field, unit, isUp]) => {
      const raw = s[field]
      if (raw === undefined || raw === null) return

      const absVal = Math.abs(raw)
      let fmt = typeof raw === 'number'
        ? absVal.toLocaleString(undefined, { maximumFractionDigits: 3 })
        : String(raw)

      // Special formatting
      if (field === 'avg_trade_duration') {
        fmt = fmtDuration(raw)
        div.innerHTML += `
          <div class="stat-row">
            <span class="stat-key">${key}</span>
            <span class="stat-val">${fmt}</span>
          </div>`
        return
      }

      let cls = ''
      if (isUp !== null) {
        if (raw > 0)      cls = isUp  ? 'up' : 'dn'
        else if (raw < 0) cls = isUp  ? 'dn' : 'up'
      }

      let disp
      if (unit === '$')  disp = (raw < 0 ? '-$' : '$') + fmt
      else if (unit === '%') disp = fmt + '%'
      else disp = fmt

      div.innerHTML += `
        <div class="stat-row">
          <span class="stat-key">${key}</span>
          <span class="stat-val ${cls}">${disp}</span>
        </div>`
    })

    wrap.appendChild(div)
  })
}

// ── EQUITY & DRAWDOWN CHARTS ──────────────────────────────────────────────
function renderEquity(equity) {
  if (!equity || equity.length === 0) return

  const times   = equity.map(e => new Date(e.time * 1000).toISOString())
  const eqs     = equity.map(e => e.equity)
  const initEq  = eqs[0] || 100000

  let peak = initEq
  const peakSeries = []
  const ddSeries   = []
  eqs.forEach(v => {
    peak = Math.max(peak, v)
    peakSeries.push(peak)
    ddSeries.push(peak > 0 ? parseFloat(((peak - v) / peak * 100).toFixed(3)) : 0)
  })

  const axBase = {
    type: 'category', data: times,
    axisLabel: {
      color: '#505090', fontSize: 9, fontFamily: 'JetBrains Mono',
      formatter: v => { const d = new Date(v); return `${d.getMonth()+1}/${d.getDate()}` },
    },
    axisLine: { lineStyle: { color: '#202045' } },
    splitLine: { show: false }, axisTick: { show: false },
  }

  equityChart.setOption({
    animation: false, backgroundColor: 'transparent',
    grid: { left: 82, right: 16, top: 20, bottom: 38 },
    xAxis: axBase,
    yAxis: {
      type: 'value', scale: true,
      axisLabel: {
        color: '#505090', fontSize: 10, fontFamily: 'JetBrains Mono',
        formatter: v => '$' + (Math.abs(v) >= 1000 ? (v / 1000).toFixed(0) + 'K' : v),
      },
      splitLine: { lineStyle: { color: '#111130', type: 'dashed' } },
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      {
        type: 'slider', bottom: 2, height: 18,
        borderColor: '#202045', backgroundColor: '#0d0d1c',
        fillerColor: 'rgba(0,229,255,.07)',
        handleStyle: { color: '#00e5ff' },
        textStyle: { color: '#404070', fontFamily: 'JetBrains Mono', fontSize: 9 },
      },
    ],
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#111122', borderColor: '#252550',
      textStyle: { color: '#dde0ff', fontFamily: 'JetBrains Mono', fontSize: 11 },
      formatter: params => {
        const idx = params[0]?.dataIndex
        if (idx == null) return ''
        const eq  = eqs[idx]
        const dd  = ddSeries[idx]
        const chg = eq - initEq
        const dt  = new Date(times[idx])
        return `<div style="font-size:10px;line-height:1.9">
          <div style="color:#555;font-size:9px">${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}</div>
          <div>Equity <b style="color:#00e5ff">${fmtDollar(eq)}</b></div>
          <div>Peak   <b style="color:#7c3aed">${fmtDollar(peakSeries[idx])}</b></div>
          <div>Change <b style="color:${chg>=0?'#8670ff':'#ff0095'}">${chg>=0?'+':''}${fmtDollar(chg)}</b></div>
          <div>DD     <b style="color:#ff1744">${dd?.toFixed(2)}%</b></div>
        </div>`
      },
    },
    legend: {
      top: 4, right: 16,
      textStyle: { color: '#7070a0', fontSize: 9, fontFamily: 'JetBrains Mono' },
      itemWidth: 14, itemHeight: 2,
    },
    series: [
      {
        name: 'Equity', type: 'line', data: eqs,
        smooth: false, symbol: 'none',
        lineStyle: { color: '#00e5ff', width: 2 },
        areaStyle: {
          color: { type:'linear', x:0,y:0,x2:0,y2:1, colorStops:[
            { offset:0, color:'rgba(0,229,255,.25)' },
            { offset:1, color:'rgba(0,229,255,.02)' },
          ]},
        },
      },
      {
        name: 'Peak', type: 'line', data: peakSeries,
        smooth: false, symbol: 'none', z: 1,
        lineStyle: { color: '#7c3aed', width: 1, type: 'dashed' },
      },
    ],
  }, true)

  ddChart.setOption({
    animation: false, backgroundColor: 'transparent',
    grid: { left: 82, right: 16, top: 8, bottom: 24 },
    xAxis: { ...axBase, axisLabel: { show: false } },
    yAxis: {
      type: 'value', scale: true, inverse: true,
      axisLabel: {
        color: '#505090', fontSize: 9, fontFamily: 'JetBrains Mono',
        formatter: v => v.toFixed(1) + '%',
      },
      splitLine: { lineStyle: { color: '#111130', type: 'dashed' } },
    },
    dataZoom: [{ type: 'inside', start: 0, end: 100 }],
    tooltip: { show: false },
    series: [{
      type: 'line', data: ddSeries, smooth: false, symbol: 'none',
      lineStyle: { color: '#ff1744', width: 1.2 },
      areaStyle: { color: { type:'linear',x:0,y:0,x2:0,y2:1, colorStops:[
        { offset:0, color:'rgba(255,23,68,.30)' },
        { offset:1, color:'rgba(255,23,68,.02)' },
      ]}},
    }],
  }, true)
}

// ── TRADES TABLE with sort + filter ──────────────────────────────────────
let _tradeSort = { col: 'num', dir: 1 }

function renderTradesTable(trades) {
  state.tradesAll = trades || []
  _applyTradeFilter()
}

$('trade-filter').addEventListener('input', _applyTradeFilter)

document.querySelectorAll('#trades-table th[data-sort]').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.sort
    if (_tradeSort.col === col) _tradeSort.dir *= -1
    else { _tradeSort.col = col; _tradeSort.dir = 1 }
    document.querySelectorAll('#trades-table th').forEach(h => {
      h.classList.remove('sort-asc', 'sort-desc')
    })
    th.classList.add(_tradeSort.dir === 1 ? 'sort-asc' : 'sort-desc')
    _applyTradeFilter()
  })
})

function _applyTradeFilter() {
  const filter = ($('trade-filter').value || '').toLowerCase()
  let trades   = [...state.tradesAll]

  // Filter
  if (filter) {
    trades = trades.filter(t =>
      t.side.toLowerCase().includes(filter) ||
      String(t.pnl).includes(filter)
    )
  }

  // Sort
  const col = _tradeSort.col
  const dir = _tradeSort.dir
  if (col && col !== 'num') {
    trades.sort((a, b) => {
      const av = a[col] ?? 0
      const bv = b[col] ?? 0
      return (av < bv ? -1 : av > bv ? 1 : 0) * dir
    })
  }

  $('trade-count').textContent = `${trades.length} / ${state.tradesAll.length}`

  const tbody = $('trades-body')
  tbody.innerHTML = ''
  trades.forEach((t, i) => {
    const up  = t.pnl >= 0
    const dur = (t.exit_time - t.entry_time)
    const row = document.createElement('tr')
    row.innerHTML = `
      <td style="color:var(--text3)">${state.tradesAll.indexOf(t) + 1}</td>
      <td style="color:${t.side==='LONG'?'#8670ff':'#ff0095'};font-weight:700">${t.side}</td>
      <td>${fmtDt(t.entry_time)}</td>
      <td>${fmtDt(t.exit_time)}</td>
      <td>${t.entry.toFixed(2)}</td>
      <td>${t.exit.toFixed(2)}</td>
      <td>${t.size}</td>
      <td style="color:var(--text3)">${fmtDuration(dur)}</td>
      <td class="${up ? 'td-up' : 'td-dn'}">${up ? '+' : ''}${fmtDollar(Math.abs(t.pnl))}</td>
    `
    tbody.appendChild(row)
  })
}

// ── ANALYTICS / MONTE CARLO ───────────────────────────────────────────────
function renderAnalytics(result) {
  const mc     = result.monte_carlo
  const trades = result.trades
  const s      = result.stats
  if (!mc || !mc.paths || mc.paths.length === 0) return

  const initCap = s.initial_capital

  // ── Badges ────────────────────────────────────────────────────────
  const badgeEl = $('mc-stats-bar')
  badgeEl.innerHTML = ''
  const badges = [
    ['Prob Profit',  mc.prob_profit + '%',                       mc.prob_profit  >= 50 ? 'up' : 'dn'],
    ['Prob Ruin',    mc.prob_ruin   + '%',                       mc.prob_ruin    <= 5  ? 'up' : 'dn'],
    ['P5 Final',     fmtDollar(mc.final_eq_p5,  0),              mc.final_eq_p5  >= initCap ? 'up' : 'dn'],
    ['Median Final', fmtDollar(mc.final_eq_median, 0),           mc.final_eq_median >= initCap ? 'up' : 'dn'],
    ['P95 Final',    fmtDollar(mc.final_eq_p95, 0),             'up'],
    ['Median DD',    mc.max_dd_p50 + '%',                       'warn'],
    ['P95 DD',       mc.max_dd_p95 + '%',                       'dn'],
  ]
  badges.forEach(([k, v, cls]) => {
    badgeEl.innerHTML += `
      <div class="mc-badge">
        <span class="mc-badge-key">${k}</span>
        <span class="mc-badge-val ${cls}">${v}</span>
      </div>`
  })

  const chartDefaults = { animation: false, backgroundColor: 'transparent' }
  const yAxisBase = {
    axisLabel: { color: '#404070', fontSize: 9, fontFamily: 'JetBrains Mono' },
    splitLine: { lineStyle: { color: '#111130', type: 'dashed' } },
  }
  const xAxisBase = {
    axisLabel: { color: '#404070', fontSize: 9 },
    axisLine:  { lineStyle: { color: '#202045' } },
    splitLine: { show: false },
  }
  const tooltipBase = {
    trigger: 'axis', backgroundColor: '#111122', borderColor: '#252550',
    textStyle: { fontSize: 10, fontFamily: 'JetBrains Mono' },
  }

  // ── MC Paths ──────────────────────────────────────────────────────
  const pathLen = mc.paths[0].length
  mcPathsChart.setOption({
    ...chartDefaults,
    grid: { left: 80, right: 14, top: 14, bottom: 30 },
    xAxis: { ...xAxisBase, type: 'category', data: [...Array(pathLen).keys()] },
    yAxis: {
      ...yAxisBase, type: 'value', scale: true,
      axisLabel: { ...yAxisBase.axisLabel, formatter: v => '$' + (v/1000).toFixed(0) + 'K' },
    },
    tooltip: tooltipBase,
    series: mc.paths.map((path, i) => ({
      type: 'line', data: path, smooth: false, symbol: 'none',
      lineStyle: {
        color: i < 6 ? '#00e5ff' : 'rgba(80,80,180,.12)',
        width: i < 6 ? 1.4 : 0.5,
      },
      z: i < 6 ? 5 : 1,
    })),
  }, true)

  // ── Final equity histogram ─────────────────────────────────────────
  const feVals  = mc.paths.map(p => p[p.length - 1])
  const BIN     = 20
  const fMin    = Math.min(...feVals), fMax = Math.max(...feVals)
  const fBw     = (fMax - fMin) / BIN || 1
  const fBins   = Array(BIN).fill(0)
  const fLabels = Array.from({ length: BIN }, (_, i) => Math.round(fMin + i * fBw))
  feVals.forEach(v => { fBins[Math.min(Math.floor((v - fMin) / fBw), BIN - 1)]++ })
  mcDistChart.setOption({
    ...chartDefaults,
    grid: { left: 50, right: 14, top: 14, bottom: 40 },
    xAxis: { ...xAxisBase, type: 'category', data: fLabels,
      axisLabel: { ...xAxisBase.axisLabel, rotate: 35, formatter: v => '$' + (v/1000).toFixed(0) + 'K' } },
    yAxis: { ...yAxisBase, type: 'value' },
    tooltip: tooltipBase,
    series: [{ type: 'bar', barCategoryGap: '4%',
      data: fBins.map((v, i) => ({ value: v,
        itemStyle: { color: fLabels[i] >= initCap ? 'rgba(134,112,255,.80)' : 'rgba(255,0,149,.70)' } })) }],
  }, true)

  // ── Max DD histogram ───────────────────────────────────────────────
  const ddVals  = mc.paths.map(path => {
    let pk = path[0], md = 0
    path.forEach(v => { pk = Math.max(pk, v); if (pk > 0) md = Math.max(md, (pk - v) / pk * 100) })
    return md
  })
  const dMax    = Math.max(...ddVals) || 1
  const dBw     = dMax / 20
  const dBins   = Array(20).fill(0)
  const dLabels = Array.from({ length: 20 }, (_, i) => +(i * dBw).toFixed(1))
  ddVals.forEach(v => { dBins[Math.min(Math.floor(v / dBw), 19)]++ })
  mcDdChart.setOption({
    ...chartDefaults,
    grid: { left: 50, right: 14, top: 14, bottom: 40 },
    xAxis: { ...xAxisBase, type: 'category', data: dLabels,
      axisLabel: { ...xAxisBase.axisLabel, rotate: 35, formatter: v => v + '%' } },
    yAxis: { ...yAxisBase, type: 'value' },
    tooltip: tooltipBase,
    series: [{ type: 'bar', barCategoryGap: '4%',
      data: dBins.map(v => ({ value: v, itemStyle: { color: 'rgba(255,23,68,.70)' } })) }],
  }, true)

  // ── PnL distribution ──────────────────────────────────────────────
  if (!trades || trades.length < 2) return
  const pnls   = trades.map(t => t.pnl)
  const pMin   = Math.min(...pnls), pMax = Math.max(...pnls)
  const pBw    = (pMax - pMin) / 30 || 1
  const pBins  = Array(30).fill(0)
  const pLbls  = Array.from({ length: 30 }, (_, i) => Math.round(pMin + i * pBw))
  pnls.forEach(v => { pBins[Math.min(Math.floor((v - pMin) / pBw), 29)]++ })
  pnlDistChart.setOption({
    ...chartDefaults,
    grid: { left: 50, right: 14, top: 14, bottom: 40 },
    xAxis: { ...xAxisBase, type: 'category', data: pLbls,
      axisLabel: { ...xAxisBase.axisLabel, rotate: 35, formatter: v => '$' + v } },
    yAxis: { ...yAxisBase, type: 'value' },
    tooltip: tooltipBase,
    series: [{ type: 'bar', barCategoryGap: '3%',
      data: pBins.map((v, i) => ({ value: v,
        itemStyle: { color: pLbls[i] >= 0 ? 'rgba(134,112,255,.80)' : 'rgba(255,0,149,.70)' } })) }],
  }, true)
}

// ── STRATEGY TEMPLATES ────────────────────────────────────────────────────
const TEMPLATES = [
  {
    name: '1. Order Flow Imbalance',
    tags: [
      { label: 'Order Flow', cls: 'of' },
      { label: 'Delta', cls: 'of' },
    ],
    desc: 'Enters long/short when cumulative delta AND size imbalance agree, confirmed by VWAP position. Exits on stop-loss, take-profit, or delta reversal.',
    code: `# ── Order Flow Imbalance Strategy ──────────────────────────────
#
# LONG  : avg delta > threshold + size_imb bullish + close >= VWAP
# SHORT : avg delta < -threshold + size_imb bearish + close <= VWAP
# EXIT  : SL / TP / delta reversal

def initialize(ctx):
    ctx.data['lb']       = 3      # lookback bars for delta average
    ctx.data['imb_thr']  = 0.20   # min |size_imb| for entry
    ctx.data['dpct_min'] = 15     # min |avg delta_pct| for entry
    ctx.data['sl']       = 1.5    # stop loss (points)
    ctx.data['tp']       = 3.0    # take profit (points)
    ctx.data['rev_bars'] = 2      # bars of opposite delta for exit

def on_bar(bar, ctx, history):
    lb   = ctx.data['lb']
    ith  = ctx.data['imb_thr']
    dmin = ctx.data['dpct_min']
    sl   = ctx.data['sl']
    tp   = ctx.data['tp']
    rev  = ctx.data['rev_bars']

    if ctx.bars_seen < lb + 2:
        return None

    close    = bar['close']
    delta    = bar.get('delta', 0) or 0
    size_imb = bar.get('size_imb', 0) or 0
    vwap     = bar.get('vwap') or close

    recent   = history[-lb:]
    avg_dpct = sum((h.get('delta_pct', 0) or 0) for h in recent) / lb

    # ── EXIT ─────────────────────────────────────────────────────
    if ctx.position != 0:
        ep = ctx.entry_price
        if ctx.position > 0:
            if close <= ep - sl: return ('CLOSE', 0)
            if close >= ep + tp: return ('CLOSE', 0)
            if all((h.get('delta', 0) or 0) < 0 for h in history[-rev:]):
                return ('CLOSE', 0)
        else:
            if close >= ep + sl: return ('CLOSE', 0)
            if close <= ep - tp: return ('CLOSE', 0)
            if all((h.get('delta', 0) or 0) > 0 for h in history[-rev:]):
                return ('CLOSE', 0)
        return None

    # ── ENTRY ────────────────────────────────────────────────────
    long_ok  = avg_dpct >  dmin and size_imb >  ith and close >= vwap and delta > 0
    short_ok = avg_dpct < -dmin and size_imb < -ith and close <= vwap and delta < 0

    if long_ok:  return ('BUY',  1)
    if short_ok: return ('SELL', 1)
    return None
`,
  },

  {
    name: '2. Cumulative Delta Divergence',
    tags: [
      { label: 'Order Flow', cls: 'of' },
      { label: 'Divergence', cls: 'brk' },
    ],
    desc: 'Detects divergence between price direction and cumulative delta direction over N bars. When price rises but cum_delta falls (hidden selling), go SHORT — and vice versa.',
    code: `# ── Cumulative Delta Divergence ─────────────────────────────────
#
# Divergence = price moves one way, cum_delta moves opposite
# LONG  : price down + cum_delta up   (hidden buying)
# SHORT : price up   + cum_delta down (hidden selling)

def initialize(ctx):
    ctx.data['lb']  = 5    # lookback for divergence detection
    ctx.data['sl']  = 2.0  # stop loss (points)
    ctx.data['tp']  = 4.0  # take profit (points)
    ctx.data['min_price_move'] = 1.0  # min price range to count

def on_bar(bar, ctx, history):
    lb     = ctx.data['lb']
    sl     = ctx.data['sl']
    tp     = ctx.data['tp']
    min_mv = ctx.data['min_price_move']

    if ctx.bars_seen < lb + 2:
        return None

    close = bar['close']

    # EXIT
    if ctx.position != 0:
        ep = ctx.entry_price
        if ctx.position > 0:
            if close <= ep - sl: return ('CLOSE', 0)
            if close >= ep + tp: return ('CLOSE', 0)
        else:
            if close >= ep + sl: return ('CLOSE', 0)
            if close <= ep - tp: return ('CLOSE', 0)
        return None

    past       = history[-(lb+1):-1]
    price_old  = past[0]['close']
    cdelta_old = past[0].get('cum_delta', 0) or 0
    price_new  = past[-1]['close']
    cdelta_new = past[-1].get('cum_delta', 0) or 0

    price_up  = (price_new - price_old) >  min_mv
    price_dn  = (price_new - price_old) < -min_mv
    cd_up     = (cdelta_new - cdelta_old) > 0
    cd_dn     = (cdelta_new - cdelta_old) < 0

    # Hidden buying: price fell but cd rising -> LONG
    if price_dn and cd_up:
        return ('BUY', 1)

    # Hidden selling: price rose but cd falling -> SHORT
    if price_up and cd_dn:
        return ('SELL', 1)

    return None
`,
  },

  {
    name: '3. VWAP Reversion',
    tags: [
      { label: 'Mean Rev', cls: 'rev' },
      { label: 'VWAP', cls: 'rev' },
    ],
    desc: 'Fades extreme deviations from VWAP when accompanied by exhaustion signals (delta drops). Expects price to return to VWAP as fair value.',
    code: `# ── VWAP Reversion Strategy ─────────────────────────────────────
#
# LONG  : close << VWAP by threshold AND delta reversing positive
# SHORT : close >> VWAP by threshold AND delta reversing negative
# EXIT  : touched VWAP or SL

def initialize(ctx):
    ctx.data['vwap_thr'] = 2.0  # min points away from VWAP for entry
    ctx.data['sl']       = 1.5  # stop loss (points)
    ctx.data['rev_bars'] = 1    # bars of confirming delta

def on_bar(bar, ctx, history):
    thr  = ctx.data['vwap_thr']
    sl   = ctx.data['sl']
    rev  = ctx.data['rev_bars']

    if ctx.bars_seen < 3:
        return None

    close = bar['close']
    vwap  = bar.get('vwap') or close
    delta = bar.get('delta', 0) or 0

    # EXIT: hit VWAP or stop
    if ctx.position != 0:
        ep = ctx.entry_price
        if ctx.position > 0:
            if close <= ep - sl:   return ('CLOSE', 0)
            if close >= vwap:      return ('CLOSE', 0)   # reached VWAP
        else:
            if close >= ep + sl:   return ('CLOSE', 0)
            if close <= vwap:      return ('CLOSE', 0)
        return None

    dist = close - vwap   # negative = below VWAP

    # Confirmation: recent delta agrees with expected bounce
    rec_delta = [h.get('delta', 0) or 0 for h in history[-rev:]]
    avg_delta = sum(rec_delta) / max(len(rec_delta), 1)

    if dist < -thr and avg_delta > 0:
        return ('BUY', 1)    # below VWAP + buy pressure

    if dist > thr and avg_delta < 0:
        return ('SELL', 1)   # above VWAP + sell pressure

    return None
`,
  },

  {
    name: '4. Volume Breakout',
    tags: [
      { label: 'Momentum', cls: 'mom' },
      { label: 'Volume', cls: 'mom' },
    ],
    desc: 'Enters in the direction of the bar when volume spikes significantly above average AND delta confirms direction. Momentum play on institutional activity.',
    code: `# ── Volume Breakout Strategy ────────────────────────────────────
#
# Entry when:
#   - Volume this bar > N × average volume (volume spike)
#   - Delta confirms direction (buy/sell pressure)
#   - No existing position

def initialize(ctx):
    ctx.data['lb']         = 20   # bars for average volume
    ctx.data['vol_mult']   = 2.5  # volume must be this × average
    ctx.data['min_delta']  = 50   # min |delta| for entry
    ctx.data['sl']         = 2.0  # stop loss (points)
    ctx.data['tp']         = 5.0  # take profit (points)

def on_bar(bar, ctx, history):
    lb      = ctx.data['lb']
    vmult   = ctx.data['vol_mult']
    mdelta  = ctx.data['min_delta']
    sl      = ctx.data['sl']
    tp      = ctx.data['tp']

    if ctx.bars_seen < lb + 1:
        return None

    close  = bar['close']
    volume = bar.get('volume', 0) or 0
    delta  = bar.get('delta',  0) or 0

    # Average volume over lookback
    avg_vol = sum(h.get('volume', 0) or 0 for h in history[-lb:]) / lb

    # EXIT
    if ctx.position != 0:
        ep = ctx.entry_price
        if ctx.position > 0:
            if close <= ep - sl: return ('CLOSE', 0)
            if close >= ep + tp: return ('CLOSE', 0)
        else:
            if close >= ep + sl: return ('CLOSE', 0)
            if close <= ep - tp: return ('CLOSE', 0)
        return None

    # ENTRY: volume spike with delta confirmation
    is_spike = avg_vol > 0 and volume >= vmult * avg_vol

    if is_spike and delta >  mdelta:
        return ('BUY',  1)

    if is_spike and delta < -mdelta:
        return ('SELL', 1)

    return None
`,
  },

  {
    name: '5. Absorption / Exhaustion',
    tags: [
      { label: 'Order Flow', cls: 'of' },
      { label: 'Reversal', cls: 'rev' },
    ],
    desc: 'Identifies absorption patterns where large volume fails to move price (wide spread, small body), signalling that the dominant side is being absorbed. Fades the move.',
    code: `# ── Absorption / Exhaustion Strategy ───────────────────────────
#
# Absorption = large volume + small price body (energy absorbed)
# On LONG absorption: expect supply is exhausted -> go LONG
# On SHORT absorption: expect demand is exhausted -> go SHORT

def initialize(ctx):
    ctx.data['lb']         = 10   # lookback for avg volume
    ctx.data['vol_mult']   = 2.0  # min volume multiplier for spike
    ctx.data['max_body']   = 1.0  # max body size to call absorption
    ctx.data['sl']         = 2.0
    ctx.data['tp']         = 4.0

def on_bar(bar, ctx, history):
    lb      = ctx.data['lb']
    vmult   = ctx.data['vol_mult']
    max_b   = ctx.data['max_body']
    sl      = ctx.data['sl']
    tp      = ctx.data['tp']

    if ctx.bars_seen < lb + 2:
        return None

    close  = bar['close']
    opn    = bar['open']
    volume = bar.get('volume', 0) or 0
    delta  = bar.get('delta',  0) or 0

    body    = abs(close - opn)
    avg_vol = sum(h.get('volume', 0) or 0 for h in history[-lb:]) / lb

    # EXIT
    if ctx.position != 0:
        ep = ctx.entry_price
        if ctx.position > 0:
            if close <= ep - sl: return ('CLOSE', 0)
            if close >= ep + tp: return ('CLOSE', 0)
        else:
            if close >= ep + sl: return ('CLOSE', 0)
            if close <= ep - tp: return ('CLOSE', 0)
        return None

    # Absorption = big volume, tiny body
    is_spike = avg_vol > 0 and volume >= vmult * avg_vol
    is_small = body <= max_b

    if not (is_spike and is_small):
        return None

    # Bear absorption (down bar, delta negative, but absorbed) -> BUY
    if close < opn and delta < 0:
        return ('BUY',  1)

    # Bull absorption (up bar, delta positive, but absorbed) -> SELL
    if close > opn and delta > 0:
        return ('SELL', 1)

    return None
`,
  },
]