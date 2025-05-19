const mysql = require('mysql2');
const pool = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: 'Toanthang2003',
  database: 'retaildw'
});
module.exports = pool.promise();
