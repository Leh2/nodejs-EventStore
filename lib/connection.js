var net  = require("net")
  , uuid = require("node-uuid")
  , Proto = require("./proto")
  , TcpCommands = require("./tcp_commands");


/**
 * Export the Connection class
 */
module.exports = Connection;


var ExpectedVersion = {
	Any: -2,
	NoStream: -1,
	EmptyStream: -1
};


/**
 * @contructor Connection Create a connection to the EventStore (defaults to 127.0.0.1:1113)
 *
 * @param {String} ip
 * @param {Number} port
 */
function Connection(ip, port) {
  ip = ip || "127.0.0.1";
  port = port || 1113;
  this._responseCallbacks = {};
  this._leftoverPacketData = null;

  // open a socket to the EventStore server  
  this._socket = net.createConnection(port, ip);

  // set up callbacks
  this._socket.on("connect", this._connected.bind(this));
  this._socket.on("data", this._onData.bind(this));
  this._socket.on("end", this._disconnected.bind(this));

  console.log("Connecting to EventStore at " + ip + ":" + port);
}

// Creates a new guid as a buffer.
Connection._newGuid = function() {
  var id = uuid.v4();
  var buffer = new Buffer(16);
  uuid.parse(id, buffer);
  return buffer;
};

/**
 * Create a new event stream (idempotent, will return success if stream exists).
 *
 * @param {String} stream
 * @param {Function} cb
 */
Connection.prototype.createStream = function(stream, cb) {
  
  var payload = Proto.serialize("CreateStream", {
    eventStreamId: stream,
    requestId: Connection._newGuid(),
    metadata: "",
    allowForwarding: true,
    isJson: true
  });

  this._sendTcpPacket("CreateStream", payload, cb);
};

Connection.prototype._createStreamComplete = function(payload, cb) {
  var res = Proto.parse("CreateStreamCompleted", payload);
  if (res.result === "Success" || res.result === "WrongExpectedVersion") {
    cb();
  } else {
    cb(res.result + ": " + res.message);
  }
};




/**
 * @function appendToStream Appends a set of events to a stream.
 *
 * @param {String} stream
 * @param {Number} expectedVersion
 * @param {Array}/{object} events
 * @param {Function} cb
 */
Connection.prototype.appendToStream = function(stream, expectedVersion, events, cb) { 

  if (typeof expectedVersion === "object") {
  	cb = events;
  	events = expectedVersion;
  	expectedVersion = ExpectedVersion.Any;
  }

  if (!Array.isArray(events)) {
  	events = [events];
  }
  
  var payload = Proto.serialize("WriteEvents", {
    eventStreamId: stream,
    expectedVersion: expectedVersion,
    events: events,
    allowForwarding: true
  });

  this._sendTcpPacket("WriteEvents", payload, cb);
};

Connection.prototype._appendToStreamComplete = function(payload, cb) {
  var res = Proto.parse("WriteEventsCompleted", payload);
  if (res.result === "Success") {
    cb(null, res.firstEventNumber);
  } else {
    cb(res.result + ": " + res.message);
  }
};


/**
* @function createEvent Create a new event in a stream.
*
* @param {String} stream
* @param {String} eventType
* @param {object} data
* @param {object} metaData
* @param {Function} cb
*/
Connection.prototype.createEvent = function(stream, eventType, data, metaData, cb) {
  var evt = {
      eventId: Connection._newGuid(),
      eventType: eventType,
      dataContentType: 1,
      metadataContentType: 0,
      data: JSON.stringify(data),
      metadata: JSON.stringify(metaData)
  };
  this.appendToStream(stream, evt, cb);
};





/**
 * @function readStreamEventsForward Raw ReadStreamEventsForward.
 *
 * @param {String} stream
 * @param {Number} start
 * @param {Number} max
 * @param {Function} cb
 */
Connection.prototype.readStreamEventsForward = function(stream, start, max, cb) {
  var payload = Proto.serialize("ReadStreamEvents", {
    eventStreamId: stream,
    fromEventNumber: start,
    maxCount: max,
    resolveLinkTos: false,
    requireMaster: false
  });
  this._sendTcpPacket("ReadStreamEventsForward", payload, cb);
};

Connection.prototype._readStreamEventsForwardComplete = function(payload, cb) {
  var res = Proto.parse("ReadStreamEventsCompleted", payload);
  cb(null, res);
};


/**
 * @function readStreamEventsBackward Raw ReadStreamEventsBackward.
 *
 * @param {String} stream
 * @param {Number} start
 * @param {Number} max
 * @param {Function} cb
 */
Connection.prototype.readStreamEventsBackward = function(stream, start, max, cb) {
  var payload = Proto.serialize("ReadStreamEvents", {
    eventStreamId: stream,
    fromEventNumber: start,
    maxCount: max,
    resolveLinkTos: false,
    requireMaster: false
  });
  this._sendTcpPacket("ReadStreamEventsBackward", payload, cb);
};

Connection.prototype._readStreamEventsBackwardComplete = function(payload, cb) {
  var res = Proto.parse("ReadStreamEventsCompleted", payload);
  cb(null, res);
};




/**
 * @function subscribeToStream Subscribes to a stream. Callback will be called for each new event.
 *
 * @param {String} stream
 * @param {Boolean} resolveLinkTos
 * @param {Function} cb
 */
Connection.prototype.subscribeToStream = function(stream, resolveLinkTos, cb, droppedCb) {
  
  if (typeof resolveLinkTos === "function") {
  	droppedCb = cb;
  	cb = resolveLinkTos;
    resolveLinkTos = false;
  }

  droppedCb = droppedCb || function() {};

  var payload = Proto.serialize("SubscribeToStream", {
    eventStreamId: stream,
    resolveLinkTos: resolveLinkTos
  });

  this._sendTcpPacket("SubscribeToStream", payload, {multi:true}, [cb,droppedCb]);
};

Connection.prototype._subscriptionConfirmation = function(payload, cb) {
  var res = Proto.parse("SubscriptionConfirmation", payload);
  console.log("Subscription confirmed", res);
  // don't call the callback unless there's an error
};

Connection.prototype._streamEventAppeared = function(payload, cb) {
  var res = Proto.parse("StreamEventAppeared", payload);
  cb[0](null, res.event.event);
};

Connection.prototype._subscriptionDropped = function(payload, cb) {
  var res = Proto.parse("SubscriptionDropped", payload);
  console.log("Subscription dropped", res);
  cb[1](res);
};







/**
 * Read all events in a stream. Callback will include all the events.
 *
 * @param {String} stream
 * @param {Function} cb
 */
Connection.prototype.readStream = function(stream, cb) {
  this.readStreamEventsForward(stream, 0, 10000000, function(err, data) {
    if (err) {
      cb(err);
      return;
    }

    var rawEvents = (data && data.events) || [];
    var events = [];
    for (i in rawEvents) {
      events.push(rawEvents[i].event);
    }
    cb(null, events);
  });
};

/**
 * Read all events in a stream, and additionally subscribe to new events.
 * Callback will be called once for each event.
 *
 * @param {String} stream
 * @param {Function} cb
 */
Connection.prototype.readAndSubscribeToStream = function(stream, cb) {
  var streaming = false;
  var queue = [];

  this.subscribeToStream(stream, function(err, ev) {
    if (streaming) {
      cb(err, ev);
    } else {
      queue.push([err, ev]);
    }
  });

  this.readStream(stream, function(err, events) {
    if (err) {
      cb(err)
      return;
    }

    // call callback for existing events
    for (var i in events) {
      cb(null, events[i]);
    }

    // call callback for new events, rejecting any duplication from overlap
    var lastEventNumber = (events.length > 0) ? events[events.length - 1].eventNumber : -1;
    for (var i in queue) {
      var err   = queue[i][0];
      var event = queue[i][1];
      if (event.eventNumber > lastEventNumber) {
        cb(err, event);
      }
    }

    // turn on streaming
    streaming = true;
    queue = [];
  });
};





Connection.prototype._connected = function() {
  console.log("Connected");
};


Connection.prototype._disconnected = function() {
  console.log("Disconnected");
};



Offsets = {};
Offsets.commandOffset = 4;
Offsets.flagsOffset = Offsets.commandOffset + 1;
Offsets.correlationOffset = Offsets.flagsOffset + 1;
Offsets.authOffset = Offsets.correlationOffset + 16;
Offsets.mandatorySize = Offsets.authOffset;


/**
 * @param {String} command 
 * @param {String} correlationId 
 * @param {Proto} payload 
 */
Connection.prototype._createTcpPacket = function(command, correlationId, payload) {
  var flags = 0;
  var payloadSize = payload ? payload.length : 0;
  var packetSize = Offsets.mandatorySize + payloadSize;
  var contentLength = packetSize - 4;
  var packet = new Buffer(packetSize);
  packet.writeUInt32LE(contentLength, 0);
  packet.writeUInt8(TcpCommands.codeForType[command], Offsets.commandOffset);
  packet.writeUInt8(flags, Offsets.flagsOffset);
  uuid.parse(correlationId, packet, Offsets.correlationOffset);
  if (payloadSize > 0) {
    payload.copy(packet, Offsets.mandatorySize);
  }  
  return packet;
};


Connection.prototype._sendTcpPacket = function(command, payload, opts, cb) {
  opts = opts || {};
  cb = cb || function() {};
  if (typeof opts === "function") {
    cb = opts;
    opts = {};
  }

  var correlationId = uuid.v4();
  this._storeResponseCallback(correlationId, opts.multi, cb);

  var packet = this._createTcpPacket(command, correlationId, payload);
  console.log("Sending " + command + " command with correlation id: " + correlationId);
  this._socket.write(packet);
};


// handle a raw TCP packet (may contain multiple ES packets, or may need to join with the next TCP packet to get a full ES packet)
Connection.prototype._onData = function(packet) {
  // if there is leftover data from the last packet, prepend it to the current packet before processing
  if (this._leftoverPacketData) {
    var newPacket = new Buffer(this._leftoverPacketData.length + packet.length);
    this._leftoverPacketData.copy(newPacket, 0);
    packet.copy(newPacket, this._leftoverPacketData.length);
    packet = newPacket;
    this._leftoverPacketData = null;
  }

  // if packet is too small to contain anything, wait for another one
  if (packet.length < 5) {
    this._leftoverPacketData = packet;
    return;
  }

  var contentLength = packet.readUInt32LE(0);
  var expectedPacketLength = contentLength + 4;
  if (packet.length === expectedPacketLength) {
    this._process(packet.slice(4));
  } else if (packet.length >= expectedPacketLength) {
    console.log("Packet too big, trying to split into multiple packets (wanted: " + expectedPacketLength + " bytes, got: " + packet.length + " bytes)");
    this._onData(packet.slice(0, expectedPacketLength));
    this._onData(packet.slice(expectedPacketLength));
  } else {
    console.log("Crap, the packet isn't big enough. Maybe there's another packet coming? (wanted: " + expectedPacketLength + " bytes, got: " + packet.length + " bytes)");
    this._leftoverPacketData = packet;
  }
};


// process an actual EventStore packet (self-contained, stripped of length header)
Connection.prototype._process = function(packet) {
  var command = TcpCommands.typeForCode[packet.readUInt8(0)];
  var correlationId = uuid.unparse(packet, 2);
  console.log("Received " + command + " command with correlation id: " + correlationId);

  var payload = null;
  var expectedPacketLength = Offsets.mandatorySize - 4;
  if (packet.length > expectedPacketLength) {
    payload = packet.slice(expectedPacketLength);
  }

  // call appropriate response handler
  var cb = this._getResponseCallback(correlationId);
  switch (command) {
    case "HeartbeatRequestCommand":
      this._sendTcpPacket("HeartbeatResponseCommand");
      cb();
      break;
    case "CreateStreamCompleted":
      this._createStreamComplete(payload, cb);
      break;
    case "WriteEventsCompleted":
      this._appendToStreamComplete(payload, cb);
      break;
    case "ReadStreamEventsForwardCompleted":
      this._readStreamEventsForwardComplete(payload, cb);
      break;
    case "ReadStreamEventsBackwardCompleted":
      this._readStreamEventsBackwardComplete(payload, cb);
      break;
    case "SubscriptionConfirmation":
      this._subscriptionConfirmation(payload, cb);
      break;
    case "SubscriptionDropped":
      this._subscriptionDropped(payload, cb);
      break;
    case "StreamEventAppeared":
      this._streamEventAppeared(payload, cb);
      break;
    default:
      console.log("Don't know how to process a " + command + " command");
      cb();
  }
};


/**
 * Store a callback with a correlation ID so it can be retrieved when the
 * response(s) come back (multi signifies N responses).
 */
Connection.prototype._storeResponseCallback = function(correlationId, multi, cb) {
  this._responseCallbacks[correlationId] = {
    multi: multi,
    cb: cb
  };
};


/**
 * Retrieve a stored response callback. Unless multi is set, delete it as well.
 */
Connection.prototype._getResponseCallback = function(correlationId) {
  var found = this._responseCallbacks[correlationId];
  if (found) {
    if (!found.multi) {
      delete this._responseCallbacks[correlationId];
    }
    return found.cb;
  } else {
    return function() {};
  }
};
