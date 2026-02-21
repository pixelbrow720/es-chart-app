/**
 * preload.js — Electron context bridge
 * Exposes safe IPC channels to the renderer process.
 * This file is REQUIRED — without it, window.electronAPI is undefined.
 */

const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('electronAPI', {
  openFileDialog:   () => ipcRenderer.invoke('open-file-dialog'),
  listDatasetFiles: () => ipcRenderer.invoke('list-dataset-files'),
})