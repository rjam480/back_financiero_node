import { connection } from "./databases/conect_mysql.js";
import fs, { read } from "fs";
import parser from "csv-parser";
import bcrypt from "bcrypt";
import axios from 'axios';
import {
  modelo,
  obtenerNombreArchivo,
  formatoNumero,
  formatoFecha,
  convertirMesNumero,
  generarPassword,
} from "./helpers/helpers.js";

let fechaActual = new Date();
fechaActual = fechaActual.toISOString().split("T")[0];

let nombreArchivo = `${process.env.FOLDER_CARGA}data_${fechaActual}.csv`;
const writeStream = fs.createWriteStream(nombreArchivo);

export const procesarCsv = async (path) => {
  const nameFile = obtenerNombreArchivo(path);

  let count = 0;
  let dataBase = [];
  let dataGiros = [];
  let dataRadicaciones = [];
  let dataUser = [];
  let userValid = [];

  let reading = fs
    .createReadStream(path, { encoding: "binary", highWaterMark: 128 * 1024 })
    .pipe(parser({ separator: ";" }))
    .on("data", (chunk) => {
      if (nameFile == "base") {
        //logica para la creacion de los usuarios y validacion si existe
       
        let existe = userValid.find((e) => e.nit.trim() == chunk["NIT"].trim());
       
        if (!existe) {
          userValid.push({ nit: chunk["NIT"] });
          const myPlaintextPassword = generarPassword();
          let fechaActual = new Date();
          fechaActual = fechaActual.toISOString().split("T")[0];
          // creacion de la fecha de expiracion del password
          let meses = parseInt(process.env.TIEMPO_EXPIRA);
          let fechaExpira = new Date();
          fechaExpira.setMonth(fechaExpira.getMonth() + meses);
          fechaExpira = fechaExpira.toISOString().split("T")[0];
          let correo = (chunk["CORREO"].trim() != '') ? (chunk["CORREO"]) : ''
          let isAdmin = (chunk["is_admin"].trim() == '')? 0 : chunk["is_admin"]
          
          dataUser.push([
            chunk["RAZON SOCIAL"],
            chunk["NIT"].trim(),
            myPlaintextPassword,
            "1",
            isAdmin,
            fechaActual,
            fechaExpira,
            correo
          ]);
        }

        // esto permite realizar la carga del archivo base
        let keysBase = Object.keys(chunk);

        keysBase.forEach((element) => {
          let nombreKeyBase = element.trim();

          if (nombreKeyBase == "GD CONTR-II") {
            chunk[nombreKeyBase] = chunk["GD CONTR-II "];
          }
        });
        
        chunk["EN PROCESO"] = formatoNumero(chunk["EN PROCESO"]);
        chunk["DEVOLUCION"] = formatoNumero(chunk["DEVOLUCION"]);
        chunk["GLOSAS"] = formatoNumero(chunk["GLOSAS"]);
        chunk["RADICACION MENSUAL"] = formatoNumero(
          chunk["RADICACION MENSUAL"]
        );
        chunk["PAGO GD SUBSIDIADO"] = formatoNumero(
          chunk["PAGO GD SUBSIDIADO"]
        );
        chunk["GD CONTR-I"] = formatoNumero(chunk["GD CONTR-II"]);
        chunk["GD CONTR-II"] = formatoNumero(chunk["GD CONTR-II"]);
        chunk["GD CONTR-III"] = formatoNumero(chunk["GD CONTR-III"]);
        chunk["GD CONTR-IV"] = formatoNumero(chunk["GD CONTR-IV"]);
        chunk["OTROS GIROS"] = formatoNumero(chunk["OTROS GIROS"]);
        chunk["PAGOS POR TESORERIA"] = formatoNumero(
          chunk["PAGOS POR TESORERIA"]
        );
        chunk["Total Giros"] = formatoNumero(chunk["Total Giros"]);
        chunk["CXP"] = formatoNumero(chunk["CXP"]);

        dataBase.push([
          chunk["NIT"],
          chunk["RAZON SOCIAL"],
          chunk["TIPO PRESTADOR"],
          chunk["ZONAL"],
          chunk["GRUPO"],
          chunk["HABILITACION"],
          chunk["RED"],
          chunk["CXP"],
          chunk["EN PROCESO"],
          chunk["DEVOLUCION"],
          chunk["GLOSAS"],
          chunk["RADICACION MENSUAL"],
          chunk["BLOQUEO"],
          chunk["PAGO GD SUBSIDIADO"],
          chunk["GD CONTR-I"],
          chunk["GD CONTR-II"],
          chunk["GD CONTR-III"],
          chunk["GD CONTR-IV"],
          chunk["OTROS GIROS"],
          chunk["PAGOS POR TESORERIA"],
          chunk["Total Giros"],
        ]);
      }
      if (nameFile == "giros") {
        let keysBase = Object.keys(chunk);

        keysBase.forEach((element) => {
          let nombreKeyBase = element.trim();

          if (nombreKeyBase == "Nombre") {
            chunk[nombreKeyBase] = chunk[" Nombre "];
            delete chunk[" Nombre "];
          }
          // if (nombreKeyBase == "Valor Giro") {
          //   chunk[nombreKeyBase] = chunk[" Valor Giro "];
          //   delete chunk[" Valor Giro "];
          // }

          if (nombreKeyBase == "Régimen") {
            chunk["regimen"] = chunk[" Régimen "];
            delete chunk[" Régimen "];
          }
        });
       
        chunk["Fecha PAGO a IPS"] = formatoFecha(chunk["Fecha PAGO a IPS"]);
        chunk["Valor Giro"] = formatoNumero(chunk["Valor Giro"]);
        chunk["mes_numero"] = convertirMesNumero(chunk["MES"]);
        dataGiros.push([
          chunk["MECANISMO"],
          chunk["Fecha PAGO a IPS"],
          chunk["Tercero"],
          chunk["Nombre"],
          chunk["Valor Giro"],
          chunk["regimen"],
          chunk["Modalidad"],
          chunk["MES"],
          chunk["mes_numero"],
          chunk["AGRUPADOR"],
        ]);
      }
      if (nameFile == "radicacion") {
        chunk["VALOR"] = formatoNumero(chunk["VALOR"]);

        dataRadicaciones.push([
          chunk["NIT"],
          chunk["RAZON_SOCIAL"],
          chunk["ANO_RADICACION"],
          chunk["MES_RADICACION"],
          chunk["ANO_INICIO_PRESTACION"],
          chunk["MES_INICIO_PRESTACION"],
          chunk["TIPO_FACTURA_AGRUPADO"],
          chunk["ESTADO_FACTURA_AGRUPADO"],
          chunk["REGIMEN_FACTURA"],
          chunk["VALOR"],
          chunk["CLASIFICACION"],
        ]);
      }
    })
    .on("end", async () => {
     
      if (nameFile == "base") {
        truncateTable(nameFile);
        chunkData(dataBase, 10000, nameFile, insertTable);
        dataBase = [];
        eliminarArchivo(path);

        // insertar la data de usuarios cada 100
        chunkData(dataUser, 100, "", insertUsers);
        
      }

      if (nameFile == "giros") {
        truncateTable(nameFile);
        chunkData(dataGiros, 10000, nameFile, insertTable);
        dataGiros = [];
        eliminarArchivo(path);
      }
      if (nameFile == "radicacion") {
        truncateTable(nameFile);
        chunkData(dataRadicaciones, 10000, nameFile, insertTable);
        dataRadicaciones = [];
        eliminarArchivo(path);
      }
    });
};
const insertTable = (path, data) => {
  const columnas = modelo[path].columnas;
  const tabla = modelo[path].tabla;
  let sql = `INSERT INTO ${tabla} (${columnas}) VALUES ?`;
  // connection.connect();
  
  connection.query(sql, [data], function (error, results, fields) {
    if (error) {
      console.log(error);
    }
    console.log(`registros insterdados con exito ${results.affectedRows}`);
  });
  
 
};

 const  insertUsers  =  async (nameFile = "", data) => {
  
  let dataJob= [];
  let i = 0
  data.forEach(async (element, index) => {
    let sql = `SELECT * FROM users WHERE nit=?`;
    let nit = element[1];
    let nuevoIndex  = index +1
    const result = await promiseCheckUser(sql,nit)

    if (result.length == 0) {
      
      if (element[7] != '') {
        
        const dataEmail = {
          nit,
          password:element[2],
          email:element[7]
        }
        dataJob.push(dataEmail)
      }
      
      
      

      if (data.length == nuevoIndex) {
        
        if (dataJob.length > 0) {
          const headers = {
            'Content-Type': 'application/json',
          }
          
          const form = new FormData()
          form.append('data',JSON.stringify(dataJob))
          dataJob = []
  
          await axios
          .post('http://back_financiero.test/api/creacion-cuenta', form,{
            headers
          })
        }
       
      }
      crearCsv(data);
      bcrypt.hash(element[2], 12, function (err, hash) {
        element[2] = hash.replace("$2b", "$2y");
        let sql = `INSERT INTO users (name,nit,password,estado,is_admin,created_at,expira_password,email) VALUES (?)`;
        connection.query(sql, [element], function (error, results, fields) {
          if (error) {
            console.log(error);
          }

          
          // console.log(
          //   `usuarios insterdados con exito ${results.affectedRows}`
          // );
        });
      });

    }
  });

  
  // envio para la creacion de las tareas
};

const chunkData = async(data, chunk, nameFile, insertFTable) => {
  let i, j, temparray;
  for (i = 0, j = data.length; i < j; i += chunk) {
    temparray = data.slice(i, i + chunk);
    
    await insertFTable(nameFile, temparray);
 
   
  }
};


const truncateTable = (path) => {
  const tabla = modelo[path].tabla;
  let sql = `TRUNCATE ${tabla}`;
  connection.query(sql, function (error, results, fields) {
    console.log("borrado de la tabla completado");
  });
};

const crearCsv = (data) => {
 
  const rows = data.map(
    (user) => `${user[0]}, ${user[1]}, ${user[2]}, ${user[5]}`
  );
  writeStream.write(`name,nit,password,fecha_creacion \n`);
  writeStream.write(`${rows.join("\n")}`);
};

const eliminarArchivo = (path) => {
  fs.unlink(path, (err) => {
    if (err) {
      console.error(`Error al remover el archivo: ${err}`);
    }
    console.log(`archivo ${path} Eliminado exitosamente`);
  });
};


const  promiseCheckUser = (sql,nit) =>{

  return new Promise((resolve, reject) => {
    connection.query(sql, [nit], function (error, results, fields) {
      if (error) {
        reject(error)
      }

      resolve(results)

    });
  });
}