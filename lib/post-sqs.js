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

function getQueueUrl(target, personId) {
  const shardNum = getShardNum(target, personId)
  return `https://sqs.${target.region}.amazonaws.com/${target.externalId}/${target.destinationBaseQueueName}_${shardNum}.fifo`
}

function getTarget(targets, eventType) {
  //Note- if there are two targets registered with the same event type, this will only route to the first one
  //TODO- build a dict rather than searching through the targets every time
  return targets.find((target) => target.eventTypes.includes(eventType))
}
//********
// This function sends messages to Amazon SQS
function send(service, targets, record, callback) {
  console.log("logging targets in send")
  console.log(targets)
  const messageBody = record.data.toString()
  const dataJson = JSON.parse(messageBody)
  const personId = dataJson.userId
  const eventType = dataJson.eventType
  const eventId = dataJson.eventId
  const target = getTarget(targets, eventType)
  const queueUrl = getQueueUrl(target, personId)

  if (!target) {
    // TODO- send this to stats(?)
    console.log(`Processed record with eventType ${eventType} with no corresponding target`)
  } else {
      console.log(`calling sendMessage for ${queueUrl}`)
      service.sendMessage({ QueueUrl: queueUrl, MessageBody: messageBody, MessageGroupId: personId, MessageDeduplicationId: eventId }, callback);
  }
};

exports.create = create;
exports.getTarget = getTarget;
exports.send = send;
exports.getQueueUrl = getQueueUrl
exports.getShardNum = getShardNum;
