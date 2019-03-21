SwiftMQ Examples
================

This directory contains JMS and AMQP example programs for SwiftMQ. 

There are 3 subdirectories, each containing scripts to start
an example. The scripts are named 'starter' resp. 'starter.bat'.
To start the SimpleQueueSender, for example, the command is

starter SimpleQueueSender testqueue@router

router_network
--------------

Contains JMS examples to be executed within a router network.
First, start 2 routers (named: router1, router2). Then you can
invoke the examples with the starter script. Defaults are used 
if you don't specify parameters.

The directory contains point-to-point (prefixed as P2P) and
publish-subscribe examples (prefixed as PubSub).

P2P sender are sending to 'testqueue@router2', PubSub publisher
are publishing to 'testtopic'. Both destinations are defined
per default on both routers. You can change the destination etc
when specifying parameters when you invoke the example. 

amqp
----

Contains examples to demonstrate the SwiftMQ AMQP 1.0 Client.
They can be used with a single router as well as with a 
router network.


cli
---

Contains examples on how to use the CLI Admin API and CLI Message Interface.


