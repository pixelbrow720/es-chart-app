const { app, BrowserWindow, ipcMain, dialog } = require('electron')
const { spawn } = require('child_process')
const path = require('path')
const http = require('http')
const fs   = require('fs')

let mainWindow
let backendProcess

// ── Start Python FastAPI backend ─────────────────────────────────────────
function startBackend() {
  const backendPath = path.join(__dirname, 'backend', 'main.py')
  const python = process.platform === 'win32' ? 'python' : 'python3'

  backendProcess = spawn(python, [backendPath], {
    stdio: ['pipe', 'pipe', 'pipe']
  })

  backendProcess.stdout.on('data', d => console.log('[backend]', d.toString()))
  backendProcess.stderr.on('data', d => console.log('[backend err]', d.toString()))
  backendProcess.on('close', code => console.log('[backend] exited:', code))
}

// ── Wait until backend is ready ──────────────────────────────────────────
function waitForBackend(retries = 20) {
  return new Promise((resolve, reject) => {
    const check = (n) => {
      http.get('http://127.0.0.1:8765/docs', res => {
        resolve()
      }).on('error', () => {
        if (n <= 0) return reject(new Error('Backend not ready'))
        setTimeout(() => check(n - 1), 500)
      })
    }
    check(retries)
  })
}

// ── Create main window ───────────────────────────────────────────────────
async function createWindow() {
  startBackend()

  try {
    await waitForBackend()
  } catch (e) {
    console.error('Backend failed to start')
  }

  mainWindow = new BrowserWindow({
    width: 1600,
    height: 960,
    minWidth: 1100,
    minHeight: 700,
    backgroundColor: '#080810',
    titleBarStyle: process.platform === 'darwin' ? 'hiddenInset' : 'default',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
    icon: path.join(__dirname, 'renderer', 'icon.png'),
  })

  mainWindow.loadFile(path.join(__dirname, 'renderer', 'index.html'))
  // mainWindow.webContents.openDevTools()
}

// ── IPC: open file dialog ─────────────────────────────────────────────────
ipcMain.handle('open-file-dialog', async () => {
  const result = await dialog.showOpenDialog(mainWindow, {
    title: 'Pilih file TBBO parquet',
    filters: [{ name: 'Parquet', extensions: ['parquet'] }],
    properties: ['openFile'],
  })
  return result.canceled ? null : result.filePaths[0]
})

// ── IPC: list parquet files in dataset/ folder ────────────────────────────
ipcMain.handle('list-dataset-files', async () => {
  const dir = path.join(__dirname, 'dataset')
  if (!fs.existsSync(dir)) return []
  try {
    return fs.readdirSync(dir)
      .filter(f => f.toLowerCase().endsWith('.parquet'))
      .map(f => ({ name: f, path: path.join(dir, f) }))
  } catch (e) {
    console.error('Error scanning dataset folder:', e)
    return []
  }
})

app.whenReady().then(createWindow)

app.on('window-all-closed', () => {
  if (backendProcess) backendProcess.kill()
  if (process.platform !== 'darwin') app.quit()
})

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow()
})