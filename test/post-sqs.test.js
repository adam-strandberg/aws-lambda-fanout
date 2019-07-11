var PostSQS = require('../lib/post-sqs.js');

var assert = require('assert');

describe('post-sqs', function() {
  describe('#getShardNum', function() {
    it('returns the personId mod shardCount', function() {
      var target = {
        shardCount: 2
      };
      var personId = "5-Ab0"
      assert.strictEqual(PostSQS.getShardNum(target, personId), 0)
      personId = "5-Ab1"
      assert.strictEqual(PostSQS.getShardNum(target, personId), 1)
    });
  });

  describe('#queueUrl', function() {
    it('constructs the correct Url to forward SQS messages to', function() {
      var record = {
        data: {
          payload: {
            personId: "1"
          }
        }
      }
      var target = {
        region: "us-east-1",
        externalId: "accountNum",
        shardCount: 2,
        destinationBaseQueueName: "base_queue_name"
      }
      assert.strictEqual(PostSQS.queueUrl(target, record), "https://sqs.us-east-1.amazonaws.com/accountNum/base_queue_name_1.fifo")
    })
  })
})