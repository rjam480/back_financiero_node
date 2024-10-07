
import "dotenv/config";

export const modelo = {
  base: {
    columnas: [
      "nit",
      "razon_social",
      "tipo_prestador",
      "zonal",
      "grupo",
      "habitacion",
      "red",
      "cxp",
      "en_proceso",
      "devolucion",
      "glosas",
      "radicacion_mensual",
      "bloqueo",
      "pago_gd_subsidiado",
      "gd_contr_i",
      "gd_contr_ii",
      "gd_contr_iii",
      "gd_contr_iv",
      "otros_giros",
      "pago_por_tesoreria",
      "total_giros",
    ],
    tabla:"bases"
  },
  giros: {
    columnas: [
      "mecanismo",
      "fecha_pagos_ips",
      "nit",
      "razon_social",
      "valor_giro",
      "regimen",
      "modalidad",
      "mes",
      "mes_numero",
      "agrupador",
    ],
    tabla:'giros'
  },
  radicacion: {
    columnas: [
      "nit",
      "razon_social",
      "anio_radicacion",
      "mes_radicacion",
      "anio_inicio_prestacion",
      "mes_inicio_prestacion",
      "tipo_factura_agrupado",
      "estado_factura_agrupado",
      "regimen_factura",
      "valor",
      "clasificacion",
    ],
    tabla:"radicaciones"
  },
};
const mesesNumero ={
    'enero':1,
    'febrero':2,
    'marzo':3,
    'abril':4,
    'mayo':5,
    'junio':6,
    'julio':7,
    'agosto':8,
    'septiembre':9,
    'octubre':10,
    'noviembre':11,
    'diciembre':12,

}
export const obtenerNombreArchivo = (path) => {
  let archivo = path.split("\\")[process.env.LECTURA_CARPETA].split(".")[0];
  archivo = archivo.toLowerCase()
  return archivo
};

export const formatoNumero = (numero) => {
    let numeroString = numero.replaceAll('.','')
    let numeroFloat =  parseFloat(numeroString).toFixed(2)
    return numeroFloat
}
export const formatoFecha =  (fecha) =>{
    let fechaFomato = fecha.split('/')
    let dia = fechaFomato[0]
    dia =  (dia <=9) ? `0${dia}` : dia
    let mes = fechaFomato[1]
    let anio = fechaFomato[2]
    let fechaNueva= `${anio}-${mes}-${dia}`
    return fechaNueva
}
export const convertirMesNumero = (mes) =>{
    let mesString = mes.toLowerCase().trim()

    return mesesNumero[mesString]
}

export const generarPassword = () => {
  return Math.random().toString(36).slice(-8);
}


