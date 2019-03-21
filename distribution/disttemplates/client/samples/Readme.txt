SwiftMQ Examples
================

This directory contains JMS and AMQP example programs for SwiftMQ. 

There are 3 subdirectories, each containing scripts to start
an example. The scripts are named 'starter' resp. 'starter.bat'.
To start the SimpleQueueSender, for example, the command is

starter SimpleQueueSender testqueue@router1

sun_examples
------------

Contains JMS examples original from Sun Microsystems. 
Some programs are a bit modified to ensure, for example,
that a subscriber thread is in place before a publisher
thread starts publishing. The file 'SampleUtilities.java'
is modified regarding the JNDI lookup.

This examples are intended to use with a SINGLE router. To
use it, start first router1 (scripts/<platform>/smqr1) and
then the appropriate example(s). Please consult the Readme
file from Sun which is stored in the directory.

For point-to-point examples use 'testqueue@router1', for 
publish-subscribe examples use 'testtopic'. Both destinations
are defined per default on both router1 and router2.

If you like to use a router network, check out the 
router_network subdirectory.


router_network
--------------

Contains JMS examples to be executed within a router network.
First, start both routers (smqr1 and smqr2). Then you can
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


