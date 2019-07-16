/* 
 * AWS Lambda Fan-Out Utility
 * 
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 */

/* 
 * This AWS Lambda Node.js function receives records from an Amazon Kinesis Stream
 *  or an Amazon DynamoDB Stream, and sends them to other endpoints as defined in
 *  its configuration table (check configuration.js for details).
 */

// Modules
var transformation = require('./lib/transformation.js');
var configuration = require('./lib/configuration.js');
var statistics = require('./lib/statistics.js');
var services = require('./lib/services.js');
var async = require('async');

// Service configuration
var config = {
	parallelTargets    : 2,    // Number of parallel targets for fan-out destination
	parallelPosters    : 2,    // Number of parallel posters for fan-out destination
	debug              : false // Activate debug messages
};
configuration.configure(config);
statistics.configure(config);
services.configure(config);

//********
function postToService(serviceReference, targets, records, stats, callback) {
	// var parallelPosters = target.parallel ? config.parallelPosters : 1;
  var parallelPosters = 1;
	var errors = [];
  var definition = serviceReference.definition;
  var service = serviceReference.service;
  var limits = definition.limits;

	var maxRecords = limits.maxRecords;
	var maxSize = limits.maxSize;
	var maxUnitSize = limits.maxUnitSize;
  var includeKey = limits.includeKey;
	var listOverhead = limits.listOverhead;
	var recordOverhead = limits.recordOverhead;
	var interRecordOverhead = limits.interRecordOverhead;

  // Filter invalid records
	records = records.filter(function (record) {
    var size = record.size + (includeKey ? Buffer.byteLength(record.key) : 0);
		if((size + listOverhead + recordOverhead) > maxUnitSize) {
			console.error("Record too large to be pushed to target");
			errors.push(new Error("Record too large, was removed"));
			return false;
		} else {
			return true;
		}
	});

  var queue = async.queue(function(record, done) {
    definition.send(service, targets, record, done);
  }, parallelPosters);

  queue.drain = function() {
    serviceReference.dispose();
    callback((errors.length > 0) ? new Error("An error occured while pushing data to an AWS Service"): null);
  };

  // Add all targets to the queue
  console.log("AKS-counting records")
  console.log(records.length)
  records.forEach(function(record) {
    console.log("AKS-pushing records")
    console.log(record)
    queue.push(record, function(err) {
      if(err) {
        errors.push(err);
        console.error("An error occured while pushing data to target");
      }
    });
  });
}

//********
// This function manages the messages for a target
function sendMessages(targets, records, stats, callback) {
  // TODO- add stats back in
  // stats.addTick('targets#' + eventSourceARN);
  // stats.register('records#' + eventSourceARN + '#' + target.destination, 'Records', 'stats', 'Count', eventSourceARN, target.destination);
  // stats.addValue('records#' + eventSourceARN + '#' + target.destination, event.Records.length);
  console.log('invoking sendMessages')

  async.waterfall([
      function(done) { services.get(targets, done); },
      function(serviceReference, done) { 
        var definition = serviceReference.definition;
        if (definition.send) {
          transformation.transformRecords(records, targets, function(err, transformedRecords) {
            postToService(serviceReference, targets, transformedRecords, stats, done);
          });
        } else {
          done(new Error("Invalid module '" + target.type + "', it must export a 'send' method"));
        }
      }
    ], function(err) {
      if(err) {
        console.error("Error while processing target '" + target.id + "': " + err);
        callback(new Error("Error while processing target '" + target.id + "': " + err));
        return;
      }
      callback();
    });
}

//********
// Lambda entry point. Loads the configuration and does the fanOut
exports.handler = function(event, context) {
  var stats = statistics.create();
  stats.register('sources', 'Sources', 'counter', 'Count'); // source, destination
  stats.register('records', 'Records', 'counter', 'Count'); // source, destination

  if (config.debug) {
    console.log("Starting process of " + event.Records.length + " events");
  }

  var hasError = false;

  async.waterfall([
      function(done) {  
        configuration.get(services.definitions, done); 
      },
      function(targets, done) { 
        sendMessages(targets, event.Records, stats, done); 
      }
    ],
    function(err) {
      if(err) {
        console.error("Error while processing events", err);
        hasError = true;
      }
    }
  );
};
