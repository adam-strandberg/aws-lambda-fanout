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
 * This Node.js library processes messages and forwards them to Amazon SQS.
 */

// Modules
var AWS = require('aws-sdk');

// Default values
var defaultValues = {
  debug: false // Activate debug messages
};

var config = {};

function configure(values) {
  if(values) {
    for(var key in values) {
      config[key] = values[key];
    }
  }
};
exports.configure = configure;
configure(defaultValues);

// Limits for message publication
exports.limits = {
  maxRecords: Number.MAX_VALUE,  // No limit on number of records, we collapse them in a single value
  maxSize: 256*1024,             // Amazon SQS only accepts up to 256KiB per message
  maxUnitSize: 256*1024,         // Amazon SQS only accepts up to 256KiB per message
  includeKey: false,             // Records will not include the key
  listOverhead: 14,              // Records are put in a JSON object "{"Records":[]}"
  recordOverhead: 0,             // Records are just serialized
  interRecordOverhead: 1         // Records are comma separated
};

// This function is used to validate the destination in a target. This is used by the configuration
exports.destinationRegex = /^https:\/\/((queue)|(sqs\.[a-z]+-[a-z]+-[0-9]))\.amazonaws\.com\/[0-9]{12}\/[a-zA-Z0-9_-]{1,80}?(\.fifo)$/;

//********
// This function creates an instance of an Amazon SQS service
function create(target, options) {
  var service = new AWS.SQS(options);
  if(config.debug) {
    console.log("Created new AWS.SQS service instance");
  }
  return service;
};

function getShardNum(target, personId) {
  const last5digits = parseInt(personId.replace(/\W/, '').slice(-5), 36)
  return last5digits % target.shardCount
}

function queueUrl(target, record) {
  // TODO- get appropriate target from record
  const dataJson = JSON.parse(record.data.toString())
  const shardNum = getShardNum(target, dataJson.userId)
  const url = `https://sqs.${target.region}.amazonaws.com/${target.externalId}/${target.destinationBaseQueueName}_${shardNum}.fifo`
  return url
}

function getTarget(targets, record) {
  const dataJson = JSON.parse(record.data.toString())
  const eventType = dataJson.eventType
  //TODO- add uniqueness constraint in DDB on eventName
  return targets.find((target) => target.eventType === eventType)
}
//********
// This function sends messages to Amazon SQS
function send(service, targets, records, callback) {
  // TODO- get appropriate target from record
  var record = records[0]
  var target = getTarget(targets, record)

  switch(target.collapse) {
    // TODO- handle these other cases
    // case "JSON": {
    //   // We have multiple messages, collapse them in a single JSON Array
    //   var entries = { Records: records.map(function(record) { return JSON.parse(record.data.toString()); }) };
    //   service.sendMessage({ QueueUrl: queueUrl(target, record), MessageBody: JSON.stringify(entries), MessageGroupId: "fakeId", MessageDeduplicationId: Math.random().toString(36).substring(0, 13) }, callback);
    //   break;
    // }
    // case "concat-b64": {
    //   // We have multiple messages, collapse them in a single buffer
    //   var data = Buffer.concat([].concat.apply([], records.map(function(record) { return record.data; })));
    //   service.sendMessage({ QueueUrl: queueUrl(target, record), MessageBody: JSON.stringify(entries).toString('base64'), MessageGroupId: "fakeId", MessageDeduplicationId: Math.random().toString(36).substring(0, 13) }, callback);
    //   break;
    // }
    // case "concat": {
    //   // We have multiple messages, collapse them in a single buffer
    //   var data = Buffer.concat([].concat.apply([], records.map(function(record) { return record.data; })));
    //   service.sendMessage({ QueueUrl: queueUrl(target, record), MessageBody: JSON.stringify(entries).toString(), MessageGroupId: "fakeId", MessageDeduplicationId: Math.random().toString(36).substring(0, 13) }, callback);
    //   break;
    // }
    default: {
      // We have a single message, let's send it
      service.sendMessage({ QueueUrl: exports.queueUrl(target, record), MessageBody: record.data.toString(), MessageGroupId: "fakeId", MessageDeduplicationId: Math.random().toString(36).substring(0, 13) }, callback);
    }
  }
};

exports.create = create;
exports.getTarget = getTarget;
exports.send = send;
exports.queueUrl = queueUrl;
exports.getShardNum = getShardNum
