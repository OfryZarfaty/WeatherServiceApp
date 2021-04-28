const { Client } = require('pg');
const fs = require("fs");
const { release } = require('os');
const copyFrom = require('pg-copy-streams').from
const config = require('config');

/*CONNECT TO DATABASE LOCALY*/
// const client = new Client({
//     user: 'postgres',
//     host: 'localhost',
//     database: 'testdb',
//     password: 'postgres',
//     port: 5432,
// });

/*CONNECT TO HEROKU DATABASE*/
const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

client.connect();

/*CREATE TABLES*/
client.query(config.get('db.createTableOneQuery'),(err, res) => {
    if (err) {
        console.error(err);
        return;
    }
    console.log('Table 1 is successfully created');
});

client.query(config.get('db.createTableTwoQuery'), (err, res) => {
  release();
    if (err) {
        console.error(err);
        return;
    }
    console.log('Table 2 is successfully created');
});

client.query(config.get('db.createTableThreeQuery'), (err, res) => {
    if (err) {
        console.error(err);
        return;
    }
    console.log('Table 3 is successfully created');
});

/*INPUTFILES AND TARGET TABLES*/
var targetTableOne = 'file1';
var targetTableTwo = 'file2';
var targetTableThree = 'file3';

const executeQuery = (targetTable) => {
  const execute = (target, callback) => {
      client.query(`Truncate ${target}`, (err) => {
              if (err) {
                  console.log(`Error in creating read stream ${err}`);
                  client.end();
                  callback(err);
              } else {
                  console.log(`Truncated ${target}`);
                  callback(null, target);
              }
          });
  }

  execute(targetTable, (err) =>{
      if (err) return console.log(`Error in Truncate Table: ${err}`)
      var stream = client.query(copyFrom(`COPY ${targetTable} (Longitude,Latitude,forecasttime,Temperature,Precipitation) FROM STDIN WITH CSV HEADER`));
      var fileStream = fs.createReadStream(__dirname + '/' + targetTable + '.csv');

      fileStream.on('error', (err) =>{
          console.log(`Error in creating read stream ${err}`)
      });

      stream.on('finish', () => {
        console.log(`Completed loading data into ${targetTable}`)
        if(targetTable == targetTableThree){
          client.end()
        }
    });
      fileStream.pipe(stream);
  });  
};

/*POPULATE TABLES*/
executeQuery(targetTableOne);
executeQuery(targetTableTwo);
executeQuery(targetTableThree);