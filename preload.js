const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('electronAPI', {
  openFileDialog:   () => ipcRenderer.invoke('open-file-dialog'),
  listDatasetFiles: () => ipcRenderer.invoke('list-dataset-files'),
})