/* ============================================================
   ES·TBBO Suite — app.js
   No chart tab. Focused: Strategy Builder → Backtest → Analytics
   ============================================================ */

const API = 'http://127.0.0.1:8765'

let state = {
  filePath:  null,
  symbol:    null,
  barType:   'time',
  interval:  '5min',
  threshold: 1000,
  dateFrom:  null,
  dateTo:    null,
  btResult:  null,
}

let equityChart, ddChart, mcPathsChart, mcDistChart, mcDdChart, pnlDistChart
let editor

// ── helpers ───────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id)
const enc = s => encodeURIComponent(s)

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

function log(msg) {
  const el = $('strat-console')
  const ts = new Date().toLocaleTimeString()
  el.textContent += (el.textContent ? '\n' : '') + `[${ts}] ${msg}`
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

  // Init CodeMirror
  editor = CodeMirror.fromTextArea($('code-editor'), {
    mode: 'python', theme: 'dracula', lineNumbers: true,
    indentUnit: 4, tabSize: 4, indentWithTabs: false,
    lineWrapping: false, autofocus: true,
    extraKeys: { Tab: cm => cm.replaceSelection('    ') },
  })
  editor.setValue(DEFAULT_STRATEGY)
  editor.setSize('100%', '100%')

  window.addEventListener('resize', resizeAll)
  setTimeout(resizeAll, 300)

  // Auto-load dataset folder
  await scanDatasetFolder()
})

function resizeAll() {
  [equityChart, ddChart, mcPathsChart, mcDistChart, mcDdChart, pnlDistChart]
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

    // Auto-load first file
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

  // Add to dataset select if not already there
  const sel = $('dataset-select')
  const exists = [...sel.options].some(o => o.value === p)
  if (!exists) {
    const name = p.split(/[\\/]/).pop()
    const opt  = new Option(name, p)
    sel.appendChild(opt)
  }
  sel.value = p
  await loadFile(p)
})

// ── LOAD FILE ─────────────────────────────────────────────────────────────
async function loadFile(filePath) {
  setStatus('loading…', 'load')
  try {
    const res = await fetch(`${API}/load?path=${enc(filePath)}`)
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const info = await res.json()
    state.filePath = filePath

    // Populate date range
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

    // Update badges
    $('fb-rows').textContent  = info.rows.toLocaleString() + ' rows'
    $('fb-range').textContent = (info.first?.slice(0, 10) || '?') +
                                ' → ' +
                                (info.last?.slice(0, 10)  || '?')

    // Enable run buttons
    $('run-strat-btn').disabled = false
    $('bt-run-btn').disabled    = false

    const fname = filePath.split(/[\\/]/).pop()
    setStatus(`ready  •  ${info.rows.toLocaleString()} rows`, 'ok')
    log(`✅ Loaded: ${fname}\n   Rows   : ${info.rows.toLocaleString()}\n   Range  : ${info.first?.slice(0,10)} → ${info.last?.slice(0,10)}\n   Symbols: ${syms.map(s => s.symbol).join(', ')}`)
  } catch (e) {
    setStatus('error loading file', 'err')
    log(`❌ Load error: ${e.message}`)
    console.error(e)
  }
}

// ── BAR TYPE TOGGLE ───────────────────────────────────────────────────────
document.querySelectorAll('.bbt').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.bbt').forEach(b => b.classList.remove('active'))
    btn.classList.add('active')
    state.barType = btn.dataset.type

    const isTime = state.barType === 'time'
    $('time-ctrl').style.display   = isTime ? 'flex' : 'none'
    $('thresh-ctrl').style.display = isTime ? 'none'  : 'flex'

    const labels = { volume: 'Volume / bar', tick: 'Ticks / bar', range: 'Points / bar' }
    const defaults = { volume: 1000, tick: 500, range: 4 }
    $('thresh-lbl').textContent = labels[state.barType] || 'N'
    $('thresh-inp').value       = defaults[state.barType] || 1000
  })
})

// ── STRATEGY EDITOR ACTIONS ───────────────────────────────────────────────
$('tpl-btn').addEventListener('click', () => {
  if (confirm('Reset editor to default template?')) {
    editor.setValue(DEFAULT_STRATEGY)
    log('📋 Template restored.')
  }
})

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
    const file   = e.target.files[0]
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
  $('strat-console').textContent = ''
})

// ── RUN BACKTEST ──────────────────────────────────────────────────────────
$('run-strat-btn').addEventListener('click', runBacktest)
$('bt-run-btn').addEventListener('click', runBacktest)

async function runBacktest() {
  if (!state.filePath) {
    log('❌ ERROR: No dataset loaded.')
    return
  }
  const code = editor.getValue().trim()
  if (!code) {
    log('❌ ERROR: Strategy is empty.')
    return
  }

  // Disable buttons while running
  $('run-strat-btn').disabled = true
  $('bt-run-btn').disabled    = true
  setBtStatus('running…', 'load')
  log('⏳ Running backtest…')

  const payload = {
    path:            state.filePath,
    symbol:          $('sym-select').value,
    bar_type:        state.barType,
    interval:        $('tf-select').value,
    threshold:       parseFloat($('thresh-inp').value) || 1000,
    date_from:       $('date-from').value || null,
    date_to:         $('date-to').value   || null,
    strategy:        editor.getValue(),
    initial_capital: parseFloat($('p-capital').value)    || 100000,
    commission:      parseFloat($('p-commission').value) || 2,
    slippage:        parseFloat($('p-slippage').value)   || 0.25,
    contract_size:   parseFloat($('p-contract').value)   || 50,
  }

  try {
    const res    = await fetch(`${API}/backtest`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
    const result = await res.json()

    if (!result.success) {
      log('❌ BACKTEST ERROR:\n' + result.error)
      setBtStatus('error — check console', 'err')
      return
    }

    state.btResult = result
    const s = result.stats

    if (!s.total_trades) {
      log('⚠️  Backtest ran but produced 0 trades. Check your signal logic.')
      setBtStatus('0 trades — adjust strategy or date range', 'err')
      return
    }

    log(
      `✅ Backtest complete!\n` +
      `   Bar type  : ${payload.bar_type.toUpperCase()}` +
        (payload.bar_type === 'time' ? ` (${payload.interval})` : ` (threshold=${payload.threshold})`) + `\n` +
      `   Trades    : ${s.total_trades}  |  Win rate : ${s.win_rate}%\n` +
      `   Total PnL : $${Number(s.total_pnl).toLocaleString()}\n` +
      `   Sharpe    : ${s.sharpe_ratio}  |  Max DD  : ${s.max_drawdown_pct}%\n` +
      `   Calmar    : ${s.calmar_ratio}  |  Sortino : ${s.sortino_ratio}`
    )

    setBtStatus(
      `${s.total_trades} trades  |  PnL: $${Number(s.total_pnl).toLocaleString()}  |  WR: ${s.win_rate}%  |  Sharpe: ${s.sharpe_ratio}`,
      s.total_pnl >= 0 ? 'ok' : 'err'
    )

    renderStats(result.stats)
    renderEquity(result.equity)
    renderTradesTable(result.trades)
    renderAnalytics(result)

    // Auto-switch to Backtest tab
    document.querySelector('[data-tab="backtest"]').click()

  } catch (e) {
    log('❌ FETCH ERROR: ' + e.message)
    setBtStatus('fetch error', 'err')
  } finally {
    $('run-strat-btn').disabled = false
    $('bt-run-btn').disabled    = false
  }
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
        ['Max Consec. Win', 'max_consec_wins',   '',  null],
        ['Max Consec. Loss','max_consec_losses', '',  null],
      ],
    },
    {
      title: '🛡 Risk',
      rows: [
        ['Max Drawdown',   'max_drawdown_pct', '%', false],
        ['Profit Factor',  'profit_factor',    '',  true],
        ['RR Ratio',       'rr_ratio',         '',  true],
        ['Expectancy',     'expectancy',       '$',  true],
        ['Avg Trade',      'avg_trade',        '$',  true],
        ['Gross Profit',   'gross_profit',     '$',  true],
        ['Gross Loss',     'gross_loss',       '$',  false],
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
      const fmt    = typeof raw === 'number'
        ? absVal.toLocaleString(undefined, { maximumFractionDigits: 3 })
        : String(raw)

      // Color logic
      let cls = ''
      if (isUp !== null) {
        if (raw > 0)      cls = isUp  ? 'up' : 'dn'
        else if (raw < 0) cls = isUp  ? 'dn' : 'up'
        // raw === 0 → no color
      }

      // Display formatting
      let disp
      if (unit === '$') {
        disp = (raw < 0 ? '-$' : '$') + fmt
      } else if (unit === '%') {
        disp = fmt + '%'
      } else {
        disp = fmt
      }

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

  const times  = equity.map(e => new Date(e.time * 1000).toISOString())
  const eqs    = equity.map(e => e.equity)
  const initEq = eqs[0] || 100000

  // Compute peak & drawdown series
  let peak = initEq
  const peakSeries = []
  const ddSeries   = []

  eqs.forEach(v => {
    peak = Math.max(peak, v)
    peakSeries.push(peak)
    ddSeries.push(peak > 0 ? parseFloat(((peak - v) / peak * 100).toFixed(3)) : 0)
  })

  const axBase = {
    type: 'category',
    data: times,
    axisLabel: {
      color: '#505090', fontSize: 9, fontFamily: 'JetBrains Mono',
      formatter: v => {
        const d = new Date(v)
        return `${d.getMonth()+1}/${d.getDate()}`
      },
    },
    axisLine:  { lineStyle: { color: '#202045' } },
    splitLine: { show: false },
    axisTick:  { show: false },
  }

  equityChart.setOption({
    animation: false,
    backgroundColor: 'transparent',
    grid: { left: 82, right: 16, top: 20, bottom: 38 },
    xAxis: axBase,
    yAxis: {
      type: 'value', scale: true,
      axisLabel: {
        color: '#505090', fontSize: 10, fontFamily: 'JetBrains Mono',
        formatter: v => '$' + (v >= 1000 ? (v / 1000).toFixed(0) + 'K' : v),
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
        const eq  = eqs[idx]
        const dd  = ddSeries[idx]
        const chg = eq - initEq
        return `<div style="font-size:10px;line-height:1.9">
          <div style="color:#555;font-size:9px">${new Date(times[idx]).toLocaleString()}</div>
          <div>Equity <b style="color:#00e5ff">$${eq?.toLocaleString()}</b></div>
          <div>Peak   <b style="color:#7c3aed">$${peakSeries[idx]?.toLocaleString()}</b></div>
          <div>Change <b style="color:${chg>=0?'#8670ff':'#ff0095'}">${chg>=0?'+':''}$${chg?.toFixed(2)}</b></div>
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
          color: {
            type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(0,229,255,.25)' },
              { offset: 1, color: 'rgba(0,229,255,.02)' },
            ],
          },
        },
      },
      {
        name: 'Peak', type: 'line', data: peakSeries,
        smooth: false, symbol: 'none',
        lineStyle: { color: '#7c3aed', width: 1, type: 'dashed' },
        z: 1,
      },
    ],
  }, true)

  ddChart.setOption({
    animation: false,
    backgroundColor: 'transparent',
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
      name: 'Drawdown', type: 'line', data: ddSeries,
      smooth: false, symbol: 'none',
      lineStyle: { color: '#ff1744', width: 1.2 },
      areaStyle: {
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: 'rgba(255,23,68,.30)' },
            { offset: 1, color: 'rgba(255,23,68,.02)' },
          ],
        },
      },
    }],
  }, true)
}

// ── TRADES TABLE ──────────────────────────────────────────────────────────
function renderTradesTable(trades) {
  const tbody = $('trades-body')
  tbody.innerHTML = ''
  if (!trades || trades.length === 0) return

  trades.forEach((t, i) => {
    const up  = t.pnl >= 0
    const row = document.createElement('tr')
    const entryDt = new Date(t.entry_time * 1000)
    const exitDt  = new Date(t.exit_time  * 1000)
    const fmtDt   = d => d.toLocaleDateString() + ' ' +
      String(d.getHours()).padStart(2,'0') + ':' +
      String(d.getMinutes()).padStart(2,'0')

    row.innerHTML = `
      <td style="color:var(--text3)">${i + 1}</td>
      <td style="color:${t.side==='LONG'?'#8670ff':'#ff0095'};font-weight:700">${t.side}</td>
      <td>${fmtDt(entryDt)}</td>
      <td>${fmtDt(exitDt)}</td>
      <td>${t.entry.toFixed(2)}</td>
      <td>${t.exit.toFixed(2)}</td>
      <td>${t.size}</td>
      <td class="${up ? 'td-up' : 'td-dn'}">${up ? '+' : ''}$${Math.abs(t.pnl).toLocaleString(undefined,{maximumFractionDigits:2})}</td>
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

  // ── Badges bar ────────────────────────────────────────────────────
  const badgeEl = $('mc-stats-bar')
  badgeEl.innerHTML = ''
  const badges = [
    ['Prob Profit',   mc.prob_profit + '%',                        mc.prob_profit  >= 50 ? 'up' : 'dn'],
    ['Prob Ruin',     mc.prob_ruin   + '%',                        mc.prob_ruin    <= 5  ? 'up' : 'dn'],
    ['P5 Final',      '$' + Number(mc.final_eq_p5).toLocaleString(),  mc.final_eq_p5  >= initCap ? 'up' : 'dn'],
    ['Median Final',  '$' + Number(mc.final_eq_median).toLocaleString(), mc.final_eq_median >= initCap ? 'up' : 'dn'],
    ['P95 Final',     '$' + Number(mc.final_eq_p95).toLocaleString(), 'up'],
    ['Median DD',     mc.max_dd_p50 + '%',                         'warn'],
    ['P95 DD',        mc.max_dd_p95 + '%',                         'dn'],
  ]
  badges.forEach(([k, v, cls]) => {
    badgeEl.innerHTML += `
      <div class="mc-badge">
        <span class="mc-badge-key">${k}</span>
        <span class="mc-badge-val ${cls}">${v}</span>
      </div>`
  })

  const chartDefaults = {
    animation: false, backgroundColor: 'transparent',
  }
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
    yAxis: { ...yAxisBase, type: 'value', scale: true,
      axisLabel: { ...yAxisBase.axisLabel, formatter: v => '$' + (v/1000).toFixed(0) + 'K' } },
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
  const feVals   = mc.paths.map(p => p[p.length - 1])
  const BIN      = 20
  const fMin     = Math.min(...feVals)
  const fMax     = Math.max(...feVals)
  const fBw      = (fMax - fMin) / BIN || 1
  const fBins    = Array(BIN).fill(0)
  const fLabels  = Array.from({ length: BIN }, (_, i) => Math.round(fMin + i * fBw))
  feVals.forEach(v => {
    const bi = Math.min(Math.floor((v - fMin) / fBw), BIN - 1)
    fBins[bi]++
  })
  mcDistChart.setOption({
    ...chartDefaults,
    grid: { left: 50, right: 14, top: 14, bottom: 40 },
    xAxis: { ...xAxisBase, type: 'category', data: fLabels,
      axisLabel: { ...xAxisBase.axisLabel, rotate: 35,
        formatter: v => '$' + (v / 1000).toFixed(0) + 'K' } },
    yAxis: { ...yAxisBase, type: 'value' },
    tooltip: tooltipBase,
    series: [{
      type: 'bar', barCategoryGap: '4%',
      data: fBins.map((v, i) => ({
        value: v,
        itemStyle: { color: fLabels[i] >= initCap ? 'rgba(134,112,255,.80)' : 'rgba(255,0,149,.70)' },
      })),
    }],
  }, true)

  // ── Max DD histogram ───────────────────────────────────────────────
  const ddVals = mc.paths.map(path => {
    let pk = path[0], md = 0
    path.forEach(v => {
      pk = Math.max(pk, v)
      if (pk > 0) md = Math.max(md, (pk - v) / pk * 100)
    })
    return md
  })
  const dMax   = Math.max(...ddVals) || 1
  const dBw    = dMax / 20
  const dBins  = Array(20).fill(0)
  const dLabels = Array.from({ length: 20 }, (_, i) => +(i * dBw).toFixed(1))
  ddVals.forEach(v => {
    const bi = Math.min(Math.floor(v / dBw), 19)
    dBins[bi]++
  })
  mcDdChart.setOption({
    ...chartDefaults,
    grid: { left: 50, right: 14, top: 14, bottom: 40 },
    xAxis: { ...xAxisBase, type: 'category', data: dLabels,
      axisLabel: { ...xAxisBase.axisLabel, rotate: 35, formatter: v => v + '%' } },
    yAxis: { ...yAxisBase, type: 'value' },
    tooltip: tooltipBase,
    series: [{
      type: 'bar', barCategoryGap: '4%',
      data: dBins.map(v => ({
        value: v,
        itemStyle: { color: 'rgba(255,23,68,.70)' },
      })),
    }],
  }, true)

  // ── PnL distribution ──────────────────────────────────────────────
  if (!trades || trades.length < 2) return
  const pnls   = trades.map(t => t.pnl)
  const pMin   = Math.min(...pnls)
  const pMax   = Math.max(...pnls)
  const pBw    = (pMax - pMin) / 30 || 1
  const pBins  = Array(30).fill(0)
  const pLbls  = Array.from({ length: 30 }, (_, i) => Math.round(pMin + i * pBw))
  pnls.forEach(v => {
    const bi = Math.min(Math.floor((v - pMin) / pBw), 29)
    pBins[bi]++
  })
  pnlDistChart.setOption({
    ...chartDefaults,
    grid: { left: 50, right: 14, top: 14, bottom: 40 },
    xAxis: { ...xAxisBase, type: 'category', data: pLbls,
      axisLabel: { ...xAxisBase.axisLabel, rotate: 35, formatter: v => '$' + v } },
    yAxis: { ...yAxisBase, type: 'value' },
    tooltip: tooltipBase,
    series: [{
      type: 'bar', barCategoryGap: '3%',
      data: pBins.map((v, i) => ({
        value: v,
        itemStyle: { color: pLbls[i] >= 0 ? 'rgba(134,112,255,.80)' : 'rgba(255,0,149,.70)' },
      })),
    }],
  }, true)
}

// ── DEFAULT STRATEGY TEMPLATE ─────────────────────────────────────────────
const DEFAULT_STRATEGY = `# ── Order Flow Imbalance Strategy ──────────────────────────────
#
# Konsep:
#   DELTA     = buy_vol - sell_vol per bar (+ = buying pressure)
#   SIZE_IMB  = (bid_sz - ask_sz) / (bid_sz + ask_sz)
#               +1 = bid stack dominan (bullish)
#               -1 = ask stack dominan (bearish)
#   CUM_DELTA = kumulatif delta (divergence vs price = sinyal kuat)
#
# Entry:
#   LONG  : avg delta positif + size_imb > threshold + close > VWAP
#   SHORT : avg delta negatif + size_imb < -threshold + close < VWAP
#
# Exit:
#   - Stop loss  1.5 points
#   - Take profit 3.0 points
#   - Delta reversal N bar berturut

def initialize(ctx):
    ctx.data['lb']         = 3      # bars untuk smooth delta
    ctx.data['imb_thr']    = 0.20   # min |size_imb| untuk entry
    ctx.data['dpct_min']   = 15     # min |delta_pct| rata-rata
    ctx.data['sl']         = 1.5    # stop loss (points)
    ctx.data['tp']         = 3.0    # take profit (points)
    ctx.data['rev_bars']   = 2      # reversal bars untuk exit

def on_bar(bar, ctx, history):
    lb    = ctx.data['lb']
    ith   = ctx.data['imb_thr']
    dmin  = ctx.data['dpct_min']
    sl    = ctx.data['sl']
    tp    = ctx.data['tp']
    rev   = ctx.data['rev_bars']

    if ctx.bars_seen < lb + 2:
        return None

    close    = bar['close']
    delta    = bar.get('delta', 0) or 0
    size_imb = bar.get('size_imb', 0) or 0
    vwap     = bar.get('vwap') or close

    recent   = history[-lb:]
    avg_dpct = sum((h.get('delta_pct', 0) or 0) for h in recent) / lb

    # ── EXIT ───────────────────────────────────────────────────────
    if ctx.position != 0:
        ep = ctx.entry_price

        if ctx.position > 0:
            if close <= ep - sl: return ('CLOSE', 0)   # stop
            if close >= ep + tp: return ('CLOSE', 0)   # target
            if all((h.get('delta', 0) or 0) < 0 for h in history[-rev:]):
                return ('CLOSE', 0)

        if ctx.position < 0:
            if close >= ep + sl: return ('CLOSE', 0)   # stop
            if close <= ep - tp: return ('CLOSE', 0)   # target
            if all((h.get('delta', 0) or 0) > 0 for h in history[-rev:]):
                return ('CLOSE', 0)

        return None

    # ── ENTRY ──────────────────────────────────────────────────────
    long_ok = (avg_dpct > dmin and size_imb > ith and
               close >= vwap  and delta > 0)

    short_ok = (avg_dpct < -dmin and size_imb < -ith and
                close <= vwap   and delta < 0)

    if long_ok:  return ('BUY',  1)
    if short_ok: return ('SELL', 1)

    return None
`