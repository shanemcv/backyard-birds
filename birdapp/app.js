'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = new URL(process.argv[3]);
const hbase = require('hbase');
// require('dotenv').config()
const port = Number(process.argv[2]);

var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port, // http or https defaults
	protocol: url.protocol.slice(0, -1), // Don't want the colon
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});

//function counterToNumber(c) {
//	return Number(Buffer.from(c).readBigInt64BE());
//}
function counterToNumber(c) {
    return parseInt(c.toString(), 10);
}

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}

//hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
//	console.info(rowToMap(value))
//	console.info(value)
//})


//hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
//	console.info(rowToMap(value))
//	console.info(value)
//})


app.use(express.static('public'));
app.get('/bird_results.html',function (req, res) {
    const rowKey = req.query['state'] + '_' + req.query['species'];
    console.log('Fetching HBase row:', rowKey);
    hclient.table('smcveigh_birds_by_state_speed').row(rowKey).get((err, row) => {
        if (err) {
            console.error('Error fetching data from HBase:', err);
            return res.send('There were 0 sightings of that species in that state.');
        }

        const stats = rowToMap(row);
        if (!stats['stats:sightings']) {
            return res.send(`No sightings found for ${rowKey}`);
        }

        const template = filesystem.readFileSync('result.mustache').toString();
        const html = mustache.render(template, {
            state: req.query['state'],
            species: req.query['species'],
            sightings: stats['stats:sightings']
        });

        res.send(html);
	});
});

app.get('/biome_results.html',function (req, res) {
    const rowKey = req.query['biome'] + '_' + req.query['species'];
    console.log('Fetching HBase row:', rowKey);
    hclient.table('smcveigh_birds_by_biome_hbase').row(rowKey).get((err, row) => {
        if (err) {
            console.error('Error fetching data from HBase:', err);
            return res.send('There were no sightings of that species and biome combination.');
        }

        const stats = rowToMap(row);
        if (!stats['stats:sightings']) {
            return res.send(`No sightings found for ${rowKey}`);
        }

        const template = filesystem.readFileSync('biome.mustache').toString();
        const html = mustache.render(template, {
            biome: req.query['biome'],
            species: req.query['species'],
            sightings: stats['stats:sightings']
        });

        res.send(html);
    });
});



const { Kafka } = require('kafkajs');

const kafkajsClient = new Kafka({
	clientId: 'test-client',
	brokers: ['boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196'],
	ssl: true,
	sasl: {
		mechanism: 'scram-sha-512',
		username: 'mpcs53014-2025',
		password: 'replacewithpassword'
	},
	connectionTimeout: 10000,
	requestTimeout: 30000
});

const testConnection = async () => {
	try {
		const admin = kafkajsClient.admin();
		await admin.connect();
		console.log('✅ KafkaJS connection successful!');

		const topics = await admin.listTopics();
		console.log('Available topics:', topics);

		await admin.disconnect();
	} catch (error) {
		console.error('❌ KafkaJS connection error:', error);
	}
};
const producer= kafkajsClient.producer()
producer.connect()
testConnection()

app.get('/birds.html',function (req, res) {
	var species_val = req.query['species'];
    var state_val = req.query['stateProvince'];
	var report = {
		stateProvince: state_val,
        species: species_val
	};

	producer.send({
		topic: 'smcveigh_bird_observations',
		messages: [{ value: JSON.stringify(report)}]
	}).then(_ => res.redirect('submit-birds.html'))
		.catch(e => {
			console.error(`[example/producer] ${e.message}`, e);
			res.redirect('submit-birds.html');
		})
});

app.listen(port);
