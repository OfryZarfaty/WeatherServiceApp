const express=require('express');
const { Client } = require('pg');
const path=require('path');
const app=express();
const PORT = process.env.PORT || 3000;
const config = require('config');

/*APP SETUP*/
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname,'/views'));
app.use(express.static(__dirname + '/views'));

app.get('/', (req,res) => {
    res.render('index.ejs');
});

/*WEATHER/DATA API*/
app.get('/weather/data', async (req, res) => {
  var Latitude =req.query.lat;
  var Longitude = req.query.lan;
  const client = new Client({
      // user: 'postgres',
      // host: 'localhost',
      // database: 'testdb',
      // password: 'postgres',
      // port: 5432
      connectionString: process.env.DATABASE_URL,
      ssl: {
        rejectUnauthorized: false
      }    
  }); 
  client.connect();
  const result1 = await client.query(config.get('app.weatherDataFileOneQuery'), [Longitude,Latitude]);
  const result2 = await client.query(config.get('app.weatherDataFileTwoQuery'), [Longitude,Latitude]);
  const result3 = await client.query(config.get('app.weatherDataFileThreeQuery'), [Longitude,Latitude]);
  res.json([result1.rows[0], result2.rows[0], result3.rows[0]]);
});

/*WEATHER/SUMMARIZE API*/
app.get('/weather/summarize', async (req, res) => {
  var Latitude =req.query.lat;
  var Longitude = req.query.lan;
  const client = new Client({
      // user: 'postgres',
      // host: 'localhost',
      // database: 'testdb',
      // password: 'postgres',
      // port: 5432
      connectionString: process.env.DATABASE_URL,
      ssl: {
        rejectUnauthorized: false
      }    
  }); 
  client.connect();

  const queryMin = 'select MIN(temperature) as temperature, MIN(Precipitation) as Precipitation from (\n' +
  'select temperature,Precipitation from file1 \n' +
	'WHERE Longitude=$1 and Latitude=$2\n' +
	'UNION\n' +
	'select temperature,Precipitation from file2\n' +
	'WHERE Longitude=$3 and Latitude=$4\n' +
	'UNION\n' +
	'select temperature,Precipitation from file3\n' +
	'WHERE Longitude=$5 and Latitude=$6 ) as all_tables';

  const queryMax = 'select MAX(temperature) as temperature, MAX(Precipitation) as Precipitation from (\n' +
  'select temperature,Precipitation from file1 \n' +
	'WHERE Longitude=$1 and Latitude=$2\n' +
	'UNION\n' +
	'select temperature,Precipitation from file2\n' +
	'WHERE Longitude=$3 and Latitude=$4\n' +
	'UNION\n' +
	'select temperature,Precipitation from file3\n' +
	'WHERE Longitude=$5 and Latitude=$6 ) as all_tables';

  const min = await client.query(queryMin, [Longitude,Latitude,Longitude,Latitude,Longitude,Latitude]);
  const max = await client.query(queryMax, [Longitude,Latitude,Longitude,Latitude,Longitude,Latitude]);
  const temperature = ((min.rows[0].temperature)+(max.rows[0].temperature))/2;
  const precipitation = ((min.rows[0].precipitation)+(max.rows[0].precipitation))/2;
  res.json({max: max.rows[0],
            min: min.rows[0],
            avg: {temperature, precipitation}});
});

app.listen(PORT, () =>{
    console.log("listening on port: " + PORT);
});

