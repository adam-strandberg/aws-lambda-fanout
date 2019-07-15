var PostSQS = require('../lib/post-sqs.js');

var assert = require('assert');

describe('post-sqs', function() {
  describe('#getShardNum', function() {
    it('returns the personId mod shardCount', function() {
      const target = {
        shardCount: 2
      };
      var personId = "59a67a994c972005e4238422"
      assert.strictEqual(PostSQS.getShardNum(target, personId), 0)
      personId = "59a67a994c972005e4238423"
      assert.strictEqual(PostSQS.getShardNum(target, personId), 1)
    });
  });

  describe('#queueUrl', function() {
    it('constructs the correct Url to forward SQS messages to', function() {
      const record = {
        data: JSON.stringify({userId: "1"})
      }
      const target = {
        region: "us-east-1",
        externalId: "accountNum",
        shardCount: 2,
        destinationBaseQueueName: "base_queue_name"
      }
      assert.strictEqual(PostSQS.queueUrl(target, record), "https://sqs.us-east-1.amazonaws.com/accountNum/base_queue_name_1.fifo")
    })
  })

  describe('#getTarget', function() {
    it('gets the appropriate target for a record based on the eventType', function(){
      const targets = [
        {eventType: "ConditionsAdded"},
        {eventType: "ConditionsDeleted"}
      ]
      var record = {
        data: JSON.stringify({eventType: "ConditionsAdded"})
      }
      assert.deepStrictEqual(PostSQS.getTarget(targets, record), {eventType: "ConditionsAdded"})
      var record = {
        data: JSON.stringify({eventType: "ConditionsDeleted"})
      }
      assert.deepStrictEqual(PostSQS.getTarget(targets, record), {eventType: "ConditionsDeleted"})
    })
  })
})