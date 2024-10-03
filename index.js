import chokidar from "chokidar";
import "dotenv/config";
import {procesarCsv} from './src/carga_archivo.js'

/**
 * crear la instancia del watcher
 */
const watcher = chokidar.watch(process.env.FOLDER, {
    persistent: true,
});

watcher.on('add', async (path) => {
    procesarCsv(path)
})
