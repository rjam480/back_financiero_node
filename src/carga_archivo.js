import {connection} from './databases/conect_mysql.js'
import fs, { read } from "fs";
import parser from "csv-parser";
import {modelo,obtenerNombreArchivo,formatoNumero,formatoFecha,convertirMesNumero} from './helpers/helpers.js'
import path from 'path';
export const procesarCsv =  (path) => {
  
    /**
     * se realizar el insert en la tabla cargados para evitar cargar nuevamente el archivo
    */
    
 
   
    const nameFile = obtenerNombreArchivo(path)
    truncateTable(nameFile)
    let count =0
    let dataBase= []
    let dataGiros=[]
    let dataRadicaciones=[]
    let reading = fs
      .createReadStream(path, { encoding: "utf8", highWaterMark: 128 * 1024 })
      .pipe(parser({ separator: ";" }))
      .on("data", (chunk) => {

        if (nameFile == 'BASE') {
            let keysBase =  Object.keys(chunk)

            keysBase.forEach(element => {
                let nombreKeyBase = element.trim()

                if (nombreKeyBase == 'GD CONTR-II') {
                    chunk[nombreKeyBase] = chunk['GD CONTR-II ']
                }
            });
            chunk['EN PROCESO'] = formatoNumero(chunk['EN PROCESO'])
            chunk['DEVOLUCION'] = formatoNumero(chunk['DEVOLUCION'])
            chunk['GLOSAS'] = formatoNumero(chunk['GLOSAS'])
            chunk['RADICACION MENSUAL'] = formatoNumero(chunk['RADICACION MENSUAL'])
            chunk['PAGO GD SUBSIDIADO'] = formatoNumero(chunk['PAGO GD SUBSIDIADO'])
            chunk['GD CONTR-I'] = formatoNumero(chunk['GD CONTR-II'])
            chunk['GD CONTR-II'] = formatoNumero(chunk['GD CONTR-II'])
            chunk['GD CONTR-III'] = formatoNumero(chunk['GD CONTR-III'])
            chunk['GD CONTR-IV'] = formatoNumero(chunk['GD CONTR-IV'])
            chunk['OTROS GIROS'] = formatoNumero(chunk['OTROS GIROS'])
            chunk['PAGOS POR TESORERIA'] = formatoNumero(chunk['PAGOS POR TESORERIA'])
            chunk['Total Giros'] = formatoNumero(chunk['Total Giros'])
            chunk['CXP'] = formatoNumero(chunk['CXP'])

            dataBase.push([
                chunk['NIT'],
                chunk['RAZON SOCIAL'],
                chunk['TIPO PRESTADOR'],
                chunk['ZONAL'],
                chunk['GRUPO'],
                chunk['HABILITACION'],
                chunk['RED'],
                chunk['CXP'],
                chunk['EN PROCESO'],
                chunk['DEVOLUCION'],
                chunk['GLOSAS'],
                chunk['RADICACION MENSUAL'],
                chunk['BLOQUEO'],
                chunk['PAGO GD SUBSIDIADO'],
                chunk['GD CONTR-I'],
                chunk['GD CONTR-II'],
                chunk['GD CONTR-III'],
                chunk['GD CONTR-IV'],
                chunk['OTROS GIROS'],
                chunk['PAGOS POR TESORERIA'],
                chunk['Total Giros'],
            ])
           
        }
        if (nameFile == 'GIROS') {
            let keysBase =  Object.keys(chunk)

            keysBase.forEach(element => {
                let nombreKeyBase = element.trim()

                if (nombreKeyBase == 'Nombre') {
                    chunk[nombreKeyBase] = chunk[' Nombre ']
                    delete chunk[' Nombre ']
                }
                if (nombreKeyBase=='Valor Giro') {
                    chunk[nombreKeyBase] = chunk[' Valor Giro ']
                    delete chunk[' Valor Giro ']
                }
                if (nombreKeyBase == 'R�gimen') {
                    chunk['regimen'] = chunk[' R�gimen ']
                    delete chunk[' R�gimen ']
                }

                
            });

            chunk['Fecha PAGO a IPS'] = formatoFecha(chunk['Fecha PAGO a IPS'])
            chunk['Valor Giro'] = formatoNumero(chunk['Valor Giro'])
            chunk['mes_numero'] = convertirMesNumero(chunk['MES'])
            dataGiros.push([
                chunk['MECANISMO'],
                chunk['Fecha PAGO a IPS'],
                chunk['Tercero'],
                chunk['Nombre'],
                chunk['Valor Giro'],
                chunk['regimen'],
                chunk['Modalidad'],
                chunk['MES'],
                chunk['mes_numero'],
                chunk['AGRUPADOR'],
            ])
        }

        if (nameFile == 'RADICACION') {
            
            chunk['VALOR'] = formatoNumero(chunk['VALOR'])

            dataRadicaciones.push([
                chunk['NIT'],
                chunk['RAZON_SOCIAL'],
                chunk['ANO_RADICACION'],
                chunk['MES_RADICACION'],
                chunk['ANO_INICIO_PRESTACION'],
                chunk['MES_INICIO_PRESTACION'],
                chunk['TIPO_FACTURA_AGRUPADO'],
                chunk['ESTADO_FACTURA_AGRUPADO'],
                chunk['REGIMEN_FACTURA'],
                chunk['VALOR'],
                chunk['CLASIFICACION'],
            ])

        }
        
      })
      .on("end", async () => {
        if (nameFile == 'BASE') {
            // insertTable(nameFile,dataBase)
            let i, j, temparray, chunk = 10000;
            for (i = 0, j = dataBase.length; i < j; i += chunk) {
                        temparray = dataBase.slice(i, i + chunk);
                        insertTable(nameFile,temparray);
            }
        }
        if (nameFile == 'GIROS') {
            // insertTable(nameFile,dataGiros)
            let i, j, temparray, chunk = 10000;
            for (i = 0, j = dataGiros.length; i < j; i += chunk) {
                        temparray = dataGiros.slice(i, i + chunk);
                        insertTable(nameFile,temparray);
            }
        }
        if (nameFile == 'RADICACION') {
            let i, j, temparray, chunk = 10000;
            for (i = 0, j = dataRadicaciones.length; i < j; i += chunk) {
                        temparray = dataRadicaciones.slice(i, i + chunk);
                        insertTable(nameFile,temparray);
            }
        }
        
        fs.unlink(path, (err) => {
            if (err) {
              console.error(`Error removing file: ${err}`);
            }
            console.log(`File ${path} has been successfully removed.`);
        });
      });
};
const insertTable = (path,data) =>{
    
    const columnas = modelo[path].columnas
    const tabla = modelo[path].tabla
    let sql = `INSERT INTO ${tabla} (${columnas}) VALUES ?`
   
    connection.query(sql,[data], function (error, results, fields) {

        if (error) {
            console.log('algo ocurrio');
            console.log(error);
        }
        console.log(`registros insterdados con exito ${results.affectedRows}`)
    });
    
   
}

const truncateTable =  (path) =>{
    const tabla = modelo[path].tabla
    let sql = `TRUNCATE ${tabla}`
    connection.query(sql, function (error, results, fields) {
       console.log('borrado de la tabla completado');
       return results
    });
}