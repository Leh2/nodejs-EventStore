var fs = require("fs")
  , path = require("path")
  , Schema = require("protobuf").Schema;

// Valid types:
//   NewEvent
//   EventRecord
//   ResolvedIndexedEvent
//   ResolvedEvent
//   DeniedToRoute
//   CreateStream
//   CreateStreamCompleted
//   WriteEvents
//   WriteEventsCompleted
//   DeleteStream
//   DeleteStreamCompleted
//   ReadEvent
//   ReadEventCompleted
//   ReadStreamEvents
//   ReadStreamEventsCompleted
//   ReadAllEvents
//   ReadAllEventsCompleted
//   TransactionStart
//   TransactionStartCompleted
//   TransactionWrite
//   TransactionWriteCompleted
//   TransactionCommit
//   TransactionCommitCompleted
//   SubscribeToStream
//   SubscriptionConfirmation
//   StreamEventAppeared
//   UnsubscribeFromStream
//   SubscriptionDropped


var schema = new Schema(fs.readFileSync(path.join(__dirname, "event_store.desc")));
module.exports = {};


/**
 * Serialize an object to protobuf.
 *
 * @param {String} type
 * @param {Object} object
 */
module.exports.serialize = function(type, object) {
  var s = schema["EventStore.Client.Messages." + type];
  var res = s.serialize(object);
  s.parse(res); // ensure valid
  return res;
};


/**
 * Deserialize an object from protobuf.
 *
 * @param {String} type
 * @param {String} data
 */
module.exports.parse = function(type, data) {
  var s = schema["EventStore.Client.Messages." + type];
  return s.parse(data);
};
