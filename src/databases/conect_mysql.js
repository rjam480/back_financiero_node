import mysql from 'mysql'
import 'dotenv/config'

export const connection = mysql.createConnection({
    host: process.env.HOST_DB,
    user: process.env.USER_DB,
    port:process.env.PORT,
    database:process.env.DATABASE,
    password:process.env.PASSWORD,
})