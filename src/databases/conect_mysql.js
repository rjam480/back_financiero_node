import mysql from 'mysql'
import 'dotenv/config'

export const connection = mysql.createConnection({
    host: process.env.HOST,
    user: process.env.USER,
    port:process.env.PORT,
    database:process.env.DATABASE,
    password:process.env.PASSWORD,
})