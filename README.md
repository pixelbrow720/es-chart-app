# ES TBBO Chart — Setup Guide

## Struktur Project
```
es-chart-app/
├── backend/
│   ├── main.py          ← FastAPI server
│   └── requirements.txt
├── renderer/
│   ├── index.html       ← UI
│   └── app.js           ← Chart logic
├── main.js              ← Electron main
├── preload.js
└── package.json
```

## Setup (sekali saja)

### 1. Install Python dependencies
```bash
pip install -r backend/requirements.txt
```

### 2. Install Node/Electron
```bash
npm install
```

## Jalankan App
```bash
npm start
```

## Cara Pakai
1. Klik **Open File** → pilih `es_tbbo_fullday.parquet` atau `es_tbbo_rth.parquet`
2. Pilih **Symbol** (ESM4, ESU4, dst)
3. Set **date range** yang diinginkan
4. Pilih **bar type**: TIME / VOL / TICK / RANGE
5. Set parameter (timeframe untuk TIME, threshold untuk lainnya)
6. Klik **LOAD CHART**

## Bar Types
| Type  | Parameter | Contoh |
|-------|-----------|--------|
| TIME  | Timeframe | 1m, 5m, 15m, 30m, 1h |
| VOL   | Volume per bar | 1000 = bar baru tiap 1000 kontrak |
| TICK  | Ticks per bar  | 500 = bar baru tiap 500 transaksi |
| RANGE | Points per bar | 4 = bar baru tiap 4 points (16 ticks) |

## Notes
- Backend FastAPI berjalan di port 8765
- Data dibaca langsung dari parquet via DuckDB (tidak load ke RAM)
- Zoom: scroll mouse atau drag slider bawah
- Hover bar untuk lihat OHLCV detail di sidebar
