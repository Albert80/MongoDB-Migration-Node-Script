/*
 *  Module 3 Assignment Lab: MongoDB Migration Node Script
 *  @author: César Alberto Trejo Juárez
 *	email: cesaratj27@gmail.com
 */

// Modules or libraries
const mongodb = require('mongodb');
const async = require('async');

// Resources
const db1 = require('./m3-customer-data.json');
const db2 = require('./m3-customer-address-data.json');
const url = 'mongodb://localhost:27017/edx-course-db';
const dbName = 'edx-course-db';
const collect = 'm3-customer';
const MongoClient = mongodb.MongoClient;
let tasks = [];
const limit = parseInt(process.argv[2], 10) || 1000;

MongoClient.connect(url, (err, client) => {
	if (err) {
		console.error(err)
		return process.exit(1)
	}

	var db = client.db(dbName);
	console.log('Connected successfully to server');

	db1.forEach( (customer, index, list) => {
		db1[index] = Object.assign(customer, db2[index]);

		if(index % limit == 0){
			tasks.push( () => {
				db.collection(collect).insert(db1.splice(0, limit), 
					(error, result) =>{
						if (error) return process.exit(1)
						console.log(`Adding ${limit} elements from index: ${index}`)
				})
			});
		}
	});

	const startTime = Date.now();
	async.parallel(tasks, (error, results) => {
		if (error) console.error(error)
		console.log('Async parallel finishing...');
		const endTime = Date.now();
		console.log(`Execution time: ${endTime-startTime}`);
		console.log(results);
	});
	
	client.close();
	

});
