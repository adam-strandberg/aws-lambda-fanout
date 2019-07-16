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

  describe('#getQueueUrl', function() {
    it('constructs the correct Url to forward SQS messages to', function() {
      const personId = "1"
      const target = {
        region: "us-east-1",
        externalId: "accountNum",
        shardCount: 2,
        destinationBaseQueueName: "base_queue_name"
      }
      assert.strictEqual(PostSQS.getQueueUrl(target, personId), "https://sqs.us-east-1.amazonaws.com/accountNum/base_queue_name_1.fifo")
    })
  })

  describe('#getTarget', function() {
    it('gets the appropriate target for a record based on the eventType', function(){
      const targets = [
        {id: "health-profile", eventTypes: ["ConditionsAdded", "ConditionsDeleted"]},
        {id: "prescription", eventTypes: ["PrescriptionEvent"]}
      ]
      var eventType = "ConditionsDeleted"
      assert.strictEqual(PostSQS.getTarget(targets, eventType).id, "health-profile")
      var eventType = "PrescriptionEvent"
      assert.strictEqual(PostSQS.getTarget(targets, eventType).id, "prescription")
    })
  })

  describe('#send', function() {
    var sendMessageArgs = null
    const service = { sendMessage: (x, f) => { sendMessageArgs = x } }
    const targets = [
      {
        id: "prescription", 
        region: "us-east-1", 
        externalId: "myAccountNum",
        eventTypes: ["PrescriptionEvent"],
        destinationBaseQueueName: "destinationBaseQueueName",
        shardCount: 1
      }
    ]
    const record = {
      data: JSON.stringify({
        eventId: "ABCD",
        userId: "1",
        eventType: "PrescriptionEvent",
        payloadContent: {
          foo: "bar"
        }
      })
    }
    const callback = (x) => { return x }
    PostSQS.send(service, targets, record, callback)
    assert.deepStrictEqual(sendMessageArgs, { 
      QueueUrl: 'https://sqs.us-east-1.amazonaws.com/myAccountNum/destinationBaseQueueName_0.fifo',
      MessageBody: '{"eventId":"ABCD","userId":"1","eventType":"PrescriptionEvent","payloadContent":{"foo":"bar"}}',
      MessageGroupId: '1',
      MessageDeduplicationId: 'ABCD' 
    })
  })
})