const API = 'http://127.0.0.1:8765'
let state = {
  filePath: null, symbol: null,
  barType: 'time', interval: '5min', threshold: 1000,
  dateFrom: null, dateTo: null,
  bars: [], btResult: null,
  showVwap: true, showEma: false, emaPeriod: 20,
  showSpread: false,
}
let mainChart, volChart, spreadChart, equityChart, ddChart,
    mcPathsChart, mcDistChart, mcDdChart, pnlDistChart, editor

// ── INIT ─────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  mainChart    = echarts.init(document.getElementById('chart-el'))
  volChart     = echarts.init(document.getElementById('vol-el'))
  spreadChart  = echarts.init(document.getElementById('spread-el'))
  equityChart  = echarts.init(document.getElementById('equity-chart'))
  ddChart      = echarts.init(document.getElementById('dd-chart'))
  mcPathsChart = echarts.init(document.getElementById('mc-paths-chart'))
  mcDistChart  = echarts.init(document.getElementById('mc-dist-chart'))
  mcDdChart    = echarts.init(document.getElementById('mc-dd-chart'))
  pnlDistChart = echarts.init(document.getElementById('pnl-dist-chart'))

  editor = CodeMirror.fromTextArea(document.getElementById('code-editor'), {
    mode: 'python', theme: 'dracula', lineNumbers: true,
    indentUnit: 4, tabSize: 4, indentWithTabs: false,
    lineWrapping: false, autofocus: true,
    extraKeys: { "Tab": cm => cm.replaceSelection("    ") }
  })
  editor.setValue(DEFAULT_STRATEGY)
  editor.setSize('100%', '100%')

  // sync zoom between main & vol & spread
  mainChart.on('dataZoom', e => {
    const action = { type: 'dataZoom', start: e.start, end: e.end }
    volChart.dispatchAction(action)
    spreadChart.dispatchAction(action)
  })

  window.addEventListener('resize', resizeAll)
  setTimeout(resizeAll, 300)

  // init indicator state
  updateIndicatorUI()
})

function resizeAll() {
  [mainChart, volChart, spreadChart, equityChart, ddChart,
   mcPathsChart, mcDistChart, mcDdChart, pnlDistChart]
    .forEach(c => c && c.resize())
}

// ── TABS ─────────────────────────────────────────────────────────────────
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'))
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'))
    tab.classList.add('active')
    document.getElementById(tab.dataset.tab + '-page').classList.add('active')
    setTimeout(resizeAll, 100)
  })
})

// ── HELPERS ───────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id)
function setStatus(msg, cls = '') { $('status').textContent = msg; $('status').className = cls }
function showOverlay(show) { $('chart-overlay').classList.toggle('hidden', !show) }
function pad(n) { return String(n).padStart(2, '0') }
function enc(s) { return encodeURIComponent(s) }
function fmtPrice(v) { return v != null ? v.toFixed(2) : '—' }

// ── OPEN FILE ────────────────────────────────────────────────────────────
$('file-btn').addEventListener('click', async () => {
  const path = await window.electronAPI.openFileDialog()
  if (!path) return
  setStatus('loading...', 'load')
  try {
    // FIX: was POST, now GET
    const res  = await fetch(`${API}/load?path=${enc(path)}`)
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const info = await res.json()
    state.filePath = path
    $('file-name').textContent = path.split(/[\\/]/).pop()
    $('inf-rows').textContent  = info.rows.toLocaleString()
    $('inf-first').textContent = info.first?.slice(0, 10) || '—'
    $('inf-last').textContent  = info.last?.slice(0, 10)  || '—'
    if (info.first) {
      $('date-from').value = info.first.slice(0, 10)
      $('date-to').value   = info.last?.slice(0, 10) || ''
    }
    const sRes = await fetch(`${API}/symbols?path=${enc(path)}`)
    const syms = await sRes.json()
    $('sym-select').innerHTML = syms.map(s =>
      `<option value="${s.symbol}">${s.symbol} (${s.count.toLocaleString()})</option>`
    ).join('')
    state.symbol = syms[0]?.symbol

    $('load-btn').disabled       = false
    $('run-strat-btn').disabled  = false
    $('bt-run-btn').disabled     = false

    const fname = path.split(/[\\/]/).pop()
    setStatus(`${info.rows.toLocaleString()} rows loaded`, 'ok')
    log(`✅ File loaded: ${fname}\n   Rows  : ${info.rows.toLocaleString()}\n   Range : ${info.first?.slice(0,10)} → ${info.last?.slice(0,10)}\n   Symbols: ${syms.map(s=>s.symbol).join(', ')}`)
  } catch(e) {
    setStatus('error loading file', 'err')
    log(`❌ Load error: ${e.message}`, 'err')
    console.error(e)
  }
})

// ── BAR TYPE ─────────────────────────────────────────────────────────────
document.querySelectorAll('.bbt').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.bbt').forEach(b => b.classList.remove('active'))
    btn.classList.add('active')
    state.barType = btn.dataset.type
    const isTime = state.barType === 'time'
    $('time-ctrl').style.display   = isTime ? 'flex' : 'none'
    $('thresh-ctrl').style.display = isTime ? 'none' : 'flex'
    $('thresh-lbl').textContent = { volume:'VOL', tick:'TICKS', range:'POINTS' }[state.barType] || 'N'
    $('thresh-inp').value = { volume: 1000, tick: 500, range: 4 }[state.barType] || 1000
  })
})

// ── INDICATOR TOGGLES ────────────────────────────────────────────────────
$('toggle-vwap').addEventListener('click', () => {
  state.showVwap = !state.showVwap
  updateIndicatorUI()
  if (state.bars.length) renderChart(state.bars)
})
$('toggle-ema').addEventListener('click', () => {
  state.showEma = !state.showEma
  updateIndicatorUI()
  if (state.bars.length) renderChart(state.bars)
})
$('toggle-spread').addEventListener('click', () => {
  state.showSpread = !state.showSpread
  $('spread-el').parentElement.style.display = state.showSpread ? 'block' : 'none'
  updateIndicatorUI()
  setTimeout(resizeAll, 50)
  if (state.bars.length) renderChart(state.bars)
})
$('ema-period').addEventListener('change', () => {
  state.emaPeriod = parseInt($('ema-period').value) || 20
  if (state.showEma && state.bars.length) renderChart(state.bars)
})

function updateIndicatorUI() {
  $('toggle-vwap').classList.toggle('ind-active', state.showVwap)
  $('toggle-ema').classList.toggle('ind-active', state.showEma)
  $('toggle-spread').classList.toggle('ind-active', state.showSpread)
  $('ema-period-wrap').style.display = state.showEma ? 'flex' : 'none'
}

function calcEMA(data, period) {
  const k = 2 / (period + 1)
  const ema = []
  let prev = null
  for (const v of data) {
    if (prev === null) { prev = v; ema.push(v); continue }
    const cur = v * k + prev * (1 - k)
    ema.push(cur); prev = cur
  }
  return ema
}

// ── LOAD CHART ───────────────────────────────────────────────────────────
$('load-btn').addEventListener('click', loadChart)
async function loadChart() {
  if (!state.filePath) return
  state.symbol    = $('sym-select').value
  state.interval  = $('tf-select').value
  state.threshold = parseFloat($('thresh-inp').value)
  state.dateFrom  = $('date-from').value
  state.dateTo    = $('date-to').value
  showOverlay(true)
  $('chart-empty').classList.add('hidden')
  setStatus('fetching bars...', 'load')
  try {
    const bars = await fetchBars()
    if (!bars || bars.length === 0) {
      setStatus('no bars — expand date range', 'err')
      showOverlay(false)
      return
    }
    state.bars = bars
    renderChart(bars)
    updateBarInfo(bars[bars.length - 1])
    updateSessionInfo(bars)
    setStatus(`${bars.length.toLocaleString()} bars`, 'ok')
    log(`📊 Chart loaded: ${bars.length.toLocaleString()} bars (${state.barType.toUpperCase()})`)
  } catch(e) {
    setStatus('error', 'err')
    log(`❌ Chart error: ${e.message}`, 'err')
    console.error(e)
  } finally {
    showOverlay(false)
  }
}

async function fetchBars() {
  const p    = enc(state.filePath)
  const s    = enc(state.symbol)
  const from = state.dateFrom ? `&date_from=${state.dateFrom}` : ''
  const to   = state.dateTo   ? `&date_to=${state.dateTo}`     : ''
  let url
  if (state.barType === 'time')
    url = `${API}/bars/time?path=${p}&symbol=${s}&interval=${state.interval}${from}${to}`
  else
    url = `${API}/bars/${state.barType}?path=${p}&symbol=${s}&threshold=${state.threshold}${from}${to}`
  const res = await fetch(url)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

// ── RENDER CHART ─────────────────────────────────────────────────────────
function renderChart(bars, trades) {
  const dates  = bars.map(b => new Date(b.time * 1000).toISOString())
  const ohlc   = bars.map(b => [b.open, b.close, b.low, b.high])
  const vols   = bars.map(b => b.volume)
  const colors = bars.map(b => b.close >= b.open ? '#8670ff' : '#ff0095')

  // Indicators
  const closes = bars.map(b => b.close)
  const emaLine = state.showEma ? calcEMA(closes, state.emaPeriod) : []
  const vwapLine = state.showVwap ? bars.map(b => b.vwap) : []
  const bidLine  = bars.map(b => b.bid)
  const askLine  = bars.map(b => b.ask)

  const commonX = {
    type: 'category', data: dates,
    axisLine: { lineStyle: { color: '#252550' } },
    axisLabel: { show: false },
    splitLine: { show: false },
    axisTick: { show: false },
  }

  // Trade markers
  const markPoints = []
  if (trades && trades.length) {
    trades.forEach(t => {
      // find closest bar index for entry
      const ei = bars.findIndex(b => b.time >= t.entry_time)
      const xi = bars.findIndex(b => b.time >= t.exit_time)
      if (ei >= 0) markPoints.push({
        coord: [ei, t.side === 'LONG' ? bars[ei].low * 0.9999 : bars[ei].high * 1.0001],
        value: t.side === 'LONG' ? '▲' : '▼',
        itemStyle: { color: t.side === 'LONG' ? '#8670ff' : '#ff0095' },
        label: { show: true, color: t.side === 'LONG' ? '#00e676' : '#ff1744', fontSize: 10 }
      })
      if (xi >= 0) markPoints.push({
        coord: [xi, t.pnl > 0 ? bars[xi].high * 1.0001 : bars[xi].low * 0.9999],
        value: '✕',
        itemStyle: { color: t.pnl > 0 ? '#00e5ff' : '#ff9800' },
        label: { show: true, color: t.pnl > 0 ? '#00e5ff' : '#ff9800', fontSize: 9 }
      })
    })
  }

  const mainSeries = [
    {
      type: 'candlestick', data: ohlc, barMaxWidth: 14,
      itemStyle: { color: '#8670ff', color0: '#ff0095', borderColor: '#8670ff', borderColor0: '#ff0095' },
      markPoint: markPoints.length ? { data: markPoints, symbol: 'circle', symbolSize: 1 } : undefined,
    }
  ]
  if (state.showEma && emaLine.length) {
    mainSeries.push({
      type: 'line', name: `EMA${state.emaPeriod}`, data: emaLine,
      smooth: false, symbol: 'none',
      lineStyle: { color: '#f59e0b', width: 1.2 }, z: 3,
    })
  }
  if (state.showVwap && vwapLine.some(v => v)) {
    mainSeries.push({
      type: 'line', name: 'VWAP', data: vwapLine,
      smooth: false, symbol: 'none',
      lineStyle: { color: '#a78bfa', width: 1.2, type: 'dashed' }, z: 3,
    })
  }

  mainChart.setOption({
    animation: false,
    backgroundColor: 'transparent',
    grid: { left: 72, right: 12, top: 14, bottom: 36 },
    xAxis: {
      ...commonX,
      axisLabel: {
        show: true, color: '#404070', fontSize: 9, fontFamily: 'JetBrains Mono',
        formatter: v => {
          const d = new Date(v)
          return `${d.getMonth()+1}/${d.getDate()} ${pad(d.getHours())}:${pad(d.getMinutes())}`
        }
      }
    },
    yAxis: {
      type: 'value', scale: true, position: 'left',
      axisLine: { lineStyle: { color: '#252550' } },
      axisLabel: { color: '#7070a0', fontSize: 10, fontFamily: 'JetBrains Mono' },
      splitLine: { lineStyle: { color: '#12122a', type: 'dashed' } },
    },
    dataZoom: [
      { type: 'inside', xAxisIndex: 0, start: 75, end: 100 },
      {
        type: 'slider', xAxisIndex: 0, bottom: 0, height: 18,
        borderColor: '#252550', backgroundColor: '#0d0d1c',
        fillerColor: 'rgba(0,229,255,.07)',
        handleStyle: { color: '#00e5ff' },
        textStyle: { color: '#404070', fontFamily: 'JetBrains Mono', fontSize: 9 },
      }
    ],
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross', crossStyle: { color: '#252550' } },
      backgroundColor: '#0d0d1c', borderColor: '#252550',
      textStyle: { color: '#dde0ff', fontFamily: 'JetBrains Mono', fontSize: 11 },
      formatter: params => {
        const c = params[0]; if (!c) return ''
        const [o, cl, l, h] = c.value
        const bar = bars[c.dataIndex]
        const v   = bar?.volume || 0
        const bid = bar?.bid, ask = bar?.ask
        const spread = (bid && ask) ? (ask - bid).toFixed(2) : '—'
        const chg = ((cl - o) / o * 100).toFixed(2)
        const col = cl >= o ? '#00e676' : '#ff1744'
        return `<div style="font-size:10px;line-height:2;min-width:180px">
          <div style="color:#7070a0;border-bottom:1px solid #252550;margin-bottom:4px;padding-bottom:2px">
            ${new Date(c.axisValue).toLocaleString()}
          </div>
          <div>O <b>${o}</b>　H <b style="color:#00e676">${h}</b></div>
          <div>L <b style="color:#ff1744">${l}</b>　C <b>${cl}</b></div>
          <div>CHG <b style="color:${col}">${cl>=o?'+':''}${chg}%</b>　VOL <b>${v.toLocaleString()}</b></div>
          ${bid ? `<div style="color:#7070a0;font-size:9px;margin-top:2px">BID ${bid}　ASK ${ask}　SPR ${spread}</div>` : ''}
        </div>`
      }
    },
    legend: {
      show: state.showEma || state.showVwap,
      top: 0, right: 16,
      textStyle: { color: '#7070a0', fontSize: 9, fontFamily: 'JetBrains Mono' },
      itemWidth: 12, itemHeight: 2,
    },
    series: mainSeries,
  }, true)

  // Volume chart
  volChart.setOption({
    animation: false, backgroundColor: 'transparent',
    grid: { left: 72, right: 12, top: 4, bottom: 22 },
    xAxis: { ...commonX },
    yAxis: {
      type: 'value', scale: true, position: 'left', splitNumber: 2,
      axisLabel: {
        color: '#404070', fontSize: 9, fontFamily: 'JetBrains Mono',
        formatter: v => v >= 1000 ? (v / 1000).toFixed(0) + 'K' : v,
      },
      splitLine: { lineStyle: { color: '#12122a', type: 'dashed' } },
    },
    dataZoom: [{ type: 'inside', xAxisIndex: 0, start: 75, end: 100 }],
    tooltip: { show: false },
    series: [{
      type: 'bar', barMaxWidth: 14,
      data: vols.map((v, i) => ({ value: v, itemStyle: { color: colors[i] + '77' } })),
    }]
  }, true)

  // Spread chart (bid-ask)
  if (state.showSpread) {
    spreadChart.setOption({
      animation: false, backgroundColor: 'transparent',
      grid: { left: 72, right: 12, top: 4, bottom: 22 },
      xAxis: { ...commonX },
      yAxis: {
        type: 'value', scale: true, position: 'left', splitNumber: 2,
        axisLabel: { color: '#404070', fontSize: 9, fontFamily: 'JetBrains Mono' },
        splitLine: { lineStyle: { color: '#12122a', type: 'dashed' } },
      },
      dataZoom: [{ type: 'inside', xAxisIndex: 0, start: 75, end: 100 }],
      tooltip: { show: false },
      series: [
        { type: 'line', name: 'BID', data: bidLine, smooth: false, symbol: 'none',
          lineStyle: { color: '#00e676', width: 1 }, z: 2 },
        { type: 'line', name: 'ASK', data: askLine, smooth: false, symbol: 'none',
          lineStyle: { color: '#ff1744', width: 1 }, z: 2 },
      ]
    }, true)
  }

  // hover → update sidebar
  mainChart.on('updateAxisPointer', e => {
    const idx = e.axesInfo?.[0]?.value
    if (idx !== undefined && bars[idx]) updateBarInfo(bars[idx])
  })
}

function updateBarInfo(bar) {
  const chg = bar.close - bar.open
  const chgPct = (chg / bar.open * 100).toFixed(2)
  const up = bar.close >= bar.open
  $('inf-o').textContent = fmtPrice(bar.open)
  $('inf-h').textContent = fmtPrice(bar.high)
  $('inf-l').textContent = fmtPrice(bar.low)
  $('inf-c').textContent = fmtPrice(bar.close)
  $('inf-v').textContent = bar.volume?.toLocaleString()
  const el = $('inf-chg')
  el.textContent = `${up ? '+' : ''}${chg.toFixed(2)} (${chgPct}%)`
  el.className = `ival ${up ? 'up' : 'dn'}`
  if (bar.bid && bar.ask) {
    $('inf-bid').textContent    = fmtPrice(bar.bid)
    $('inf-ask').textContent    = fmtPrice(bar.ask)
    $('inf-spread').textContent = (bar.ask - bar.bid).toFixed(2)
  }
  if (bar.vwap) $('inf-vwap').textContent = fmtPrice(bar.vwap)
}

function updateSessionInfo(bars) {
  const h = Math.max(...bars.map(b => b.high))
  const l = Math.min(...bars.map(b => b.low))
  $('inf-bars').textContent = bars.length.toLocaleString()
  $('inf-dh').textContent   = fmtPrice(h)
  $('inf-dl').textContent   = fmtPrice(l)
  $('inf-dr').textContent   = (h - l).toFixed(2)
}

// ── STRATEGY EDITOR ──────────────────────────────────────────────────────
$('tpl-btn').addEventListener('click', () => {
  if (confirm('Reset to default template?')) {
    editor.setValue(DEFAULT_STRATEGY)
    log('📋 Template loaded.')
  }
})

$('save-btn').addEventListener('click', () => {
  const code = editor.getValue()
  const blob = new Blob([code], { type: 'text/plain' })
  const a = document.createElement('a')
  a.href = URL.createObjectURL(blob)
  a.download = 'strategy.py'
  a.click()
  log('💾 Strategy saved as strategy.py')
})

// FIX: was missing handler for load-strat-btn
$('load-strat-btn').addEventListener('click', () => {
  const input = document.createElement('input')
  input.type = 'file'
  input.accept = '.py,.txt'
  input.onchange = e => {
    const file = e.target.files[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = ev => {
      editor.setValue(ev.target.result)
      log(`📂 Loaded: ${file.name}`)
    }
    reader.readAsText(file)
  }
  input.click()
})

$('run-strat-btn').addEventListener('click', runBacktest)
$('bt-run-btn').addEventListener('click', runBacktest)

async function runBacktest() {
  if (!state.filePath) { log('❌ ERROR: No file loaded', 'err'); return }
  const code = editor.getValue()
  if (!code.trim()) { log('❌ ERROR: Empty strategy', 'err'); return }

  log('⏳ Running backtest...')
  setBtStatus('running...', 'load')

  const payload = {
    path:            state.filePath,
    symbol:          $('sym-select').value,
    bar_type:        state.barType,
    interval:        $('tf-select').value,
    threshold:       parseFloat($('thresh-inp').value || 1000),
    date_from:       $('date-from').value || null,
    date_to:         $('date-to').value   || null,
    strategy:        code,
    initial_capital: parseFloat($('p-capital').value    || 100000),
    commission:      parseFloat($('p-commission').value || 2),
    slippage:        parseFloat($('p-slippage').value   || 0.25),
    contract_size:   parseFloat($('p-contract').value   || 50),
  }

  try {
    const res    = await fetch(`${API}/backtest`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
    const result = await res.json()

    if (!result.success) {
      log('❌ BACKTEST ERROR:\n' + result.error, 'err')
      setBtStatus('error — check console', 'err')
      return
    }

    state.btResult = result
    const s = result.stats
    log(
      `✅ Backtest complete!\n` +
      `   Trades   : ${s.total_trades}  |  Win rate: ${s.win_rate}%\n` +
      `   Total PnL: $${s.total_pnl?.toLocaleString()}\n` +
      `   Sharpe   : ${s.sharpe_ratio}  |  Max DD: ${s.max_drawdown_pct}%\n` +
      `   Calmar   : ${s.calmar_ratio}  |  Sortino: ${s.sortino_ratio}`
    )
    setBtStatus(
      `${s.total_trades} trades  |  PnL: $${s.total_pnl?.toLocaleString()}  |  WR: ${s.win_rate}%  |  Sharpe: ${s.sharpe_ratio}`,
      'ok'
    )

    renderStats(result.stats)
    renderEquity(result.equity)
    renderTradesTable(result.trades)
    renderAnalytics(result)

    // show trade markers on chart if bars loaded
    if (state.bars.length) {
      renderChart(state.bars, result.trades)
    }

    document.querySelector('[data-tab="backtest"]').click()
  } catch(e) {
    log('❌ FETCH ERROR: ' + e.message, 'err')
    setBtStatus('error', 'err')
  }
}

function setBtStatus(msg, cls = '') {
  $('bt-status').textContent = msg
  $('bt-status').className   = cls
}

// ── STATS SIDEBAR ────────────────────────────────────────────────────────
function renderStats(s) {
  $('bt-hint').style.display = 'none'
  const wrap = $('bt-stats-wrap')
  wrap.innerHTML = ''
  const cards = [
    { title: 'Performance', rows: [
      ['Total Return', 'total_return_pct', '%',  true],
      ['Total PnL',   'total_pnl',        '$',  true],
      ['Final Equity','final_equity',      '$',  true],
      ['Sharpe',      'sharpe_ratio',      '',   true],
      ['Sortino',     'sortino_ratio',     '',   true],
      ['Calmar',      'calmar_ratio',      '',   true],
    ]},
    { title: 'Trades', rows: [
      ['Total Trades','total_trades',   '', null],
      ['Win Rate',    'win_rate',       '%', true],
      ['Avg Win',     'avg_win',        '$', true],
      ['Avg Loss',    'avg_loss',       '$', false],
      ['Best Trade',  'best_trade',     '$', true],
      ['Worst Trade', 'worst_trade',    '$', false],
    ]},
    { title: 'Risk', rows: [
      ['Max Drawdown','max_drawdown_pct','%', false],
      ['Profit Factor','profit_factor', '',   true],
      ['RR Ratio',    'rr_ratio',       '',   true],
      ['Expectancy',  'expectancy',     '$',  true],
      ['Max Con. W',  'max_consec_wins','',   true],
      ['Max Con. L',  'max_consec_losses','', false],
    ]},
  ]
  cards.forEach(card => {
    const div = document.createElement('div')
    div.className = 'stat-card'
    div.innerHTML = `<div class="stat-card-title">${card.title}</div>`
    card.rows.forEach(([key, field, unit, isUp]) => {
      const raw = s[field]; if (raw === undefined) return
      const val = typeof raw === 'number'
        ? raw.toLocaleString(undefined, { maximumFractionDigits: 3 })
        : raw
      const cls = isUp === null ? '' : (isUp && raw > 0 ? 'up' : (!isUp && raw < 0 ? 'dn' : isUp && raw < 0 ? 'dn' : 'up'))
      const disp = unit === '$' && raw < 0
        ? '-$' + Math.abs(raw).toLocaleString()
        : unit === '$' ? '$' + val : val + (unit || '')
      div.innerHTML += `
        <div class="stat-row">
          <span class="stat-key">${key}</span>
          <span class="stat-val ${cls}">${disp}</span>
        </div>`
    })
    wrap.appendChild(div)
  })
}

// ── EQUITY & DRAWDOWN CHARTS ─────────────────────────────────────────────
function renderEquity(equity) {
  const times  = equity.map(e => new Date(e.time * 1000).toISOString())
  const eqs    = equity.map(e => e.equity)
  const initEq = eqs[0] || 100000
  let pk = initEq
  const peak = []
  const dd = eqs.map(e => {
    pk = Math.max(pk, e); peak.push(pk)
    return parseFloat(((pk - e) / pk * 100).toFixed(3))
  })

  const axBase = {
    type: 'category', data: times,
    axisLabel: {
      color: '#404070', fontSize: 9, fontFamily: 'JetBrains Mono',
      formatter: v => new Date(v).toLocaleDateString(),
    },
    axisLine: { lineStyle: { color: '#252550' } },
    splitLine: { show: false },
  }

  equityChart.setOption({
    animation: false, backgroundColor: 'transparent',
    grid: { left: 78, right: 12, top: 14, bottom: 32 },
    xAxis: axBase,
    yAxis: {
      type: 'value', scale: true, position: 'left',
      axisLabel: {
        color: '#404070', fontSize: 10, fontFamily: 'JetBrains Mono',
        formatter: v => '$' + v.toLocaleString(),
      },
      splitLine: { lineStyle: { color: '#12122a', type: 'dashed' } },
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      {
        type: 'slider', bottom: 0, height: 18,
        borderColor: '#252550', backgroundColor: '#0d0d1c',
        fillerColor: 'rgba(0,229,255,.07)', handleStyle: { color: '#00e5ff' },
        textStyle: { color: '#404070', fontFamily: 'JetBrains Mono', fontSize: 9 },
      }
    ],
    tooltip: {
      trigger: 'axis', backgroundColor: '#0d0d1c',
      borderColor: '#252550',
      textStyle: { color: '#dde0ff', fontFamily: 'JetBrains Mono', fontSize: 11 },
    },
    legend: {
      top: 2, right: 16,
      textStyle: { color: '#7070a0', fontSize: 9, fontFamily: 'JetBrains Mono' },
      itemWidth: 14, itemHeight: 2,
    },
    series: [
      {
        name: 'Equity', type: 'line', data: eqs, smooth: false, symbol: 'none',
        lineStyle: { color: '#00e5ff', width: 1.5 },
        areaStyle: {
          color: {
            type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(0,229,255,.20)' },
              { offset: 1, color: 'rgba(0,229,255,.02)' },
            ]
          }
        },
      },
      {
        name: 'Peak', type: 'line', data: peak, smooth: false, symbol: 'none',
        lineStyle: { color: '#7c3aed', width: 1, type: 'dashed' }, z: 1,
      },
    ]
  }, true)

  ddChart.setOption({
    animation: false, backgroundColor: 'transparent',
    grid: { left: 78, right: 12, top: 8, bottom: 24 },
    xAxis: { ...axBase, axisLabel: { show: false } },
    yAxis: {
      type: 'value', scale: true, inverse: true, position: 'left',
      axisLabel: {
        color: '#404070', fontSize: 9, fontFamily: 'JetBrains Mono',
        formatter: v => v + '%',
      },
      splitLine: { lineStyle: { color: '#12122a', type: 'dashed' } },
    },
    dataZoom: [{ type: 'inside', start: 0, end: 100 }],
    tooltip: {
      trigger: 'axis', backgroundColor: '#0d0d1c', borderColor: '#252550',
      textStyle: { color: '#dde0ff', fontFamily: 'JetBrains Mono', fontSize: 11 },
    },
    series: [{
      name: 'Drawdown', type: 'line', data: dd, smooth: false, symbol: 'none',
      lineStyle: { color: '#ff1744', width: 1 },
      areaStyle: {
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: 'rgba(255,23,68,.28)' },
            { offset: 1, color: 'rgba(255,23,68,.02)' },
          ]
        }
      },
    }]
  }, true)
}

// ── TRADES TABLE ─────────────────────────────────────────────────────────
function renderTradesTable(trades) {
  const tbody = $('trades-body')
  tbody.innerHTML = ''
  trades.forEach((t, i) => {
    const up  = t.pnl > 0
    const row = document.createElement('tr')
    row.innerHTML = `
      <td>${i + 1}</td>
      <td style="color:${t.side==='LONG'?'#00e676':'#ff1744'};font-weight:700">${t.side}</td>
      <td>${new Date(t.entry_time * 1000).toLocaleString()}</td>
      <td>${new Date(t.exit_time  * 1000).toLocaleString()}</td>
      <td>${t.entry}</td>
      <td>${t.exit}</td>
      <td>${t.size}</td>
      <td class="${up ? 'td-up' : 'td-dn'}">${up ? '+' : ''}$${t.pnl.toLocaleString()}</td>`
    tbody.appendChild(row)
  })
}

// ── ANALYTICS / MONTE CARLO ──────────────────────────────────────────────
function renderAnalytics(result) {
  const mc     = result.monte_carlo
  const trades = result.trades
  if (!mc || !mc.paths) return

  const initCap = result.stats.initial_capital

  // MC Stats bar
  const bar = $('mc-stats-bar'); bar.innerHTML = ''
  const badges = [
    ['Prob Profit', mc.prob_profit + '%', mc.prob_profit >= 50 ? 'up' : 'dn'],
    ['Prob Ruin',   mc.prob_ruin   + '%', mc.prob_ruin <= 5  ? 'up' : 'dn'],
    ['Median Final', '$' + mc.final_eq_median?.toLocaleString(), mc.final_eq_median >= initCap ? 'up' : 'dn'],
    ['P5 Final',    '$' + mc.final_eq_p5?.toLocaleString(), mc.final_eq_p5 >= initCap ? 'up' : 'dn'],
    ['P95 Final',   '$' + mc.final_eq_p95?.toLocaleString(), 'up'],
    ['Median DD',   mc.max_dd_p50 + '%', 'warn'],
    ['P95 DD',      mc.max_dd_p95 + '%', 'dn'],
  ]
  badges.forEach(([k, v, cls]) => {
    bar.innerHTML += `
      <div class="mc-badge">
        <span class="mc-badge-key">${k}</span>
        <span class="mc-badge-val ${cls}">${v}</span>
      </div>`
  })

  // MC Paths
  const pathLen = mc.paths[0]?.length || 0
  const xAxis   = [...Array(pathLen).keys()]
  mcPathsChart.setOption({
    animation: false, backgroundColor: 'transparent',
    grid: { left: 78, right: 12, top: 12, bottom: 30 },
    xAxis: { type: 'category', data: xAxis, axisLabel: { color: '#404070', fontSize: 9 }, axisLine: { lineStyle: { color: '#252550' } }, splitLine: { show: false } },
    yAxis: {
      type: 'value', scale: true,
      axisLabel: { color: '#404070', fontSize: 9, fontFamily: 'JetBrains Mono', formatter: v => '$' + v.toLocaleString() },
      splitLine: { lineStyle: { color: '#12122a', type: 'dashed' } },
    },
    tooltip: { trigger: 'axis', backgroundColor: '#0d0d1c', borderColor: '#252550', textStyle: { fontSize: 10, fontFamily: 'JetBrains Mono' } },
    series: mc.paths.map((path, i) => ({
      type: 'line', data: path, smooth: false, symbol: 'none',
      lineStyle: { color: i < 5 ? '#00e5ff' : 'rgba(100,100,200,.14)', width: i < 5 ? 1.4 : 0.6 },
      z: i < 5 ? 5 : 1,
    }))
  }, true)

  // Final equity distribution histogram
  const feVals   = mc.paths.map(p => p[p.length - 1])
  const binCount = 20
  const mn  = Math.min(...feVals), mx2 = Math.max(...feVals)
  const bw  = (mx2 - mn) / binCount
  const bins = Array(binCount).fill(0)
  const binLabels = Array.from({ length: binCount }, (_, i) => Math.round(mn + i * bw))
  feVals.forEach(v => { const bi = Math.min(Math.floor((v - mn) / bw), binCount - 1); bins[bi]++ })
  mcDistChart.setOption({
    animation: false, backgroundColor: 'transparent',
    grid: { left: 56, right: 12, top: 12, bottom: 30 },
    xAxis: { type: 'category', data: binLabels, axisLabel: { color: '#404070', fontSize: 9, formatter: v => '$' + (v / 1000).toFixed(0) + 'K' }, axisLine: { lineStyle: { color: '#252550' } } },
    yAxis: { type: 'value', axisLabel: { color: '#404070', fontSize: 9 }, splitLine: { lineStyle: { color: '#12122a', type: 'dashed' } } },
    tooltip: { trigger: 'axis', backgroundColor: '#0d0d1c', borderColor: '#252550' },
    series: [{ type: 'bar', data: bins.map((v, i) => ({ value: v, itemStyle: { color: binLabels[i] >= initCap ? 'rgba(0,230,118,.75)' : 'rgba(255,23,68,.75)' } })), barCategoryGap: '5%' }]
  }, true)

  // Max DD distribution
  const ddVals = []
  mc.paths.forEach(path => {
    let pk = path[0], md = 0
    path.forEach(v => { pk = Math.max(pk, v); md = Math.max(md, (pk - v) / pk * 100) })
    ddVals.push(md)
  })
  const ddMn = 0, ddMx = Math.max(...ddVals)
  const ddBw = (ddMx - ddMn) / 20
  const ddBins   = Array(20).fill(0)
  const ddLabels = Array.from({ length: 20 }, (_, i) => +(ddMn + i * ddBw).toFixed(1))
  ddVals.forEach(v => { const bi = Math.min(Math.floor((v - ddMn) / ddBw), 19); ddBins[bi]++ })
  mcDdChart.setOption({
    animation: false, backgroundColor: 'transparent',
    grid: { left: 56, right: 12, top: 12, bottom: 30 },
    xAxis: { type: 'category', data: ddLabels, axisLabel: { color: '#404070', fontSize: 9, formatter: v => v + '%' }, axisLine: { lineStyle: { color: '#252550' } } },
    yAxis: { type: 'value', axisLabel: { color: '#404070', fontSize: 9 }, splitLine: { lineStyle: { color: '#12122a', type: 'dashed' } } },
    tooltip: { trigger: 'axis', backgroundColor: '#0d0d1c', borderColor: '#252550' },
    series: [{ type: 'bar', data: ddBins.map(v => ({ value: v, itemStyle: { color: 'rgba(255,23,68,.65)' } })), barCategoryGap: '5%' }]
  }, true)

  // Trade PnL distribution
  const pnls  = trades.map(t => t.pnl)
  const pMn   = Math.min(...pnls), pMx = Math.max(...pnls)
  const pBw   = (pMx - pMn) / 30
  const pBins   = Array(30).fill(0)
  const pLabels = Array.from({ length: 30 }, (_, i) => Math.round(pMn + i * pBw))
  pnls.forEach(v => { const bi = Math.min(Math.floor((v - pMn) / pBw), 29); pBins[bi]++ })
  pnlDistChart.setOption({
    animation: false, backgroundColor: 'transparent',
    grid: { left: 56, right: 12, top: 12, bottom: 30 },
    xAxis: { type: 'category', data: pLabels, axisLabel: { color: '#404070', fontSize: 9, formatter: v => '$' + v }, axisLine: { lineStyle: { color: '#252550' } } },
    yAxis: { type: 'value', axisLabel: { color: '#404070', fontSize: 9 }, splitLine: { lineStyle: { color: '#12122a', type: 'dashed' } } },
    tooltip: { trigger: 'axis', backgroundColor: '#0d0d1c', borderColor: '#252550' },
    series: [{ type: 'bar', data: pBins.map((v, i) => ({ value: v, itemStyle: { color: pLabels[i] >= 0 ? 'rgba(0,230,118,.75)' : 'rgba(255,23,68,.75)' } })), barCategoryGap: '3%' }]
  }, true)
}

// ── CONSOLE LOG ──────────────────────────────────────────────────────────
function log(msg, type = '') {
  const el = $('strat-console')
  const ts = new Date().toLocaleTimeString()
  el.textContent += (el.textContent ? '\n' : '') + `[${ts}] ${msg}`
  el.scrollTop = el.scrollHeight
}

$('clear-console-btn').addEventListener('click', () => {
  $('strat-console').textContent = ''
})

// ── DEFAULT STRATEGY TEMPLATE ────────────────────────────────────────────
const DEFAULT_STRATEGY = `# ── RSI + EMA Trend Strategy ───────────────────────────────────
# Entry : EMA cross + RSI filter (tidak oversold/overbought ekstrem)
# Exit  : EMA cross balik atau RSI ekstrem

def initialize(ctx):
    ctx.data['ema_fast']   = 9
    ctx.data['ema_slow']   = 21
    ctx.data['rsi_period'] = 14
    ctx.data['rsi_ob']     = 70   # overbought
    ctx.data['rsi_os']     = 30   # oversold

def _ema(prices, period):
    k, prev = 2 / (period + 1), prices[0]
    for p in prices[1:]:
        prev = p * k + prev * (1 - k)
    return prev

def _rsi(prices, period):
    if len(prices) < period + 1:
        return 50.0
    diffs  = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    recent = diffs[-period:]
    gains  = sum(d for d in recent if d > 0) / period
    losses = sum(-d for d in recent if d < 0) / period
    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100 - (100 / (1 + rs))

def on_bar(bar, ctx, history):
    fast   = ctx.data['ema_fast']
    slow   = ctx.data['ema_slow']
    rp     = ctx.data['rsi_period']
    ob, os = ctx.data['rsi_ob'], ctx.data['rsi_os']

    min_bars = slow + rp + 2
    if ctx.bars_seen < min_bars:
        return None

    closes = [h['close'] for h in history]

    ema_f  = _ema(closes[-fast*3:],  fast)
    ema_s  = _ema(closes[-slow*3:],  slow)
    rsi    = _rsi(closes, rp)

    prev_c   = closes[:-1]
    ema_f_p  = _ema(prev_c[-fast*3:], fast)
    ema_s_p  = _ema(prev_c[-slow*3:], slow)

    cross_up   = ema_f_p <= ema_s_p and ema_f > ema_s
    cross_down = ema_f_p >= ema_s_p and ema_f < ema_s

    # Entry Long: golden cross + RSI tidak overbought
    if cross_up and rsi < ob and ctx.position <= 0:
        return ('BUY', 1)

    # Entry Short: death cross + RSI tidak oversold
    if cross_down and rsi > os and ctx.position >= 0:
        return ('SELL', 1)

    # Exit Long jika RSI overbought
    if ctx.position > 0 and rsi >= ob:
        return ('CLOSE', 0)

    # Exit Short jika RSI oversold
    if ctx.position < 0 and rsi <= os:
        return ('CLOSE', 0)

    return None
`