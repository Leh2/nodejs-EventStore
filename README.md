nodejs-EventStore
=================

[Event Store](http://geteventstore.com/) is "an awesome, rock-solid, super-fast persistence engine for event-sourced applications."

It has HTTP and TCP client interfaces -- this project implements the TCP interface in Node.js in an async fashion.


Project status
--------------

_Very_ early stage. Only a subset of the TCP API is implemented. But you can create streams of events and read them back, so the most important functionality is in!


Usage
-----

Installing:

    npm install git://github.com/kenpratt/nodejs-EventStore.git


Connecting:

    var EventStore = require("nodejs-EventStore");
    var es = EventStore.connect(host, port);


Creating an event stream:

    es.createStream(stream, cb);


Appending to a stream (callback passed error and firstEventNumber):

	es.appendToStream(stream, [expectedVersion,] events, cb);


Creating an event (shortcut for appending a single event):

    es.createEvent(stream, eventType, data, metaData, cb);

Subscribing to a stream:

	es.subscribeToStream(stream, [resolveLinkTos,] cb, droppedCb);

Reading a stream:

	es.readStreamEventsForward(stream,start,max,cb);
	es.readStreamEventsBackward(stream,start,max,cb);


Reading & subscribing to an event stream (callback will be called for each existing event in the stream, and for each event added in the future):

    es.readAndSubscribeToStream(stream, cb);


Dependencies
------------

* [Node.js](http://nodejs.org/)
* A running [Event Store](http://geteventstore.com/) instance


Development dependencies
------------------------

* [Protocol Buffers (protobuf)](http://code.google.com/p/protobuf/)
* Development Node.js packages: `npm install --dev`
