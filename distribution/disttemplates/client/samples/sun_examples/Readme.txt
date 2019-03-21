This is the original ReadMe from Sun
====================================


JMS CODE EXAMPLES

The JMS code examples show how to write a simple application using JMS.  They 
demonstrate most of the important features of JMS.

The JMS examples are divided into three groups: 

 - Basic examples provide a simple introduction to JMS.  They show how to send
   and receive a single message using either a queue or a topic.

 - Intermediate examples demonstrate somewhat more complex features of JMS:
     - using message listeners for asynchronous sending and receiving of 
       messages
     - using the five supported message formats
     
 - Advanced examples demonstrate still more advanced features of JMS:
     - using the request/reply facility
     - using message headers
     - using message selectors
     - using durable subscriptions

You can run these examples using any JMS implementation, although you may need
to modify the code that looks up a connection factory and a destination.  The 
code assumes that you have connection factories with the Java Naming and 
Directory Service (JNDI) names
"QueueConnectionFactory" and "TopicConnectionFactory."  The names are specified
in the file SampleUtilities.java.
  
You can run the simpler queue examples in pairs, each program in a separate 
terminal window.  This allows you to simulate two separate applications, one
sending a message, the other receiving it.

For the other examples, the sender and receiver (or the publisher and the
subscriber, for topic examples) are each a separate class within the overall
program class.  When you run these examples, the two classes use threads to
send and receive messages within the same program.


Before You Start
================

Before you begin, you need to start the JMS service and to create a queue and
a topic.
      
Compile the sample programs individually if you wish, or all at once by using
the command

  javac *.java


What All the Examples Have in Common
====================================

All the examples use the utility class SampleUtilities.java.  It contains the
following methods:

  - A jndiLookup method that obtains a JNDI
    context and uses it to look up the names of connection factories and 
    destinations. 
    
  - A wait_for_quit method used by two of the asynchronous examples to prevent
    them from exiting before they have received all the messages sent.  Another 
    asynchronous example, TopicSelectors.java, uses a message selector to
    accomplish this task.
    
  - An exit method that all the examples call.  It is a workaround for a known
    limitation in the current JMS Reference Implementation.

Most of the JMS examples execute the same basic setup steps:

  1.  They read a topic or queue name from the command line.  The topic or 
      queue must have been created previously using the JMS service.
  
  2.  They look up a connection factory and the topic or queue using the
      jndiLookup method in the class SampleUtilities.
  
  3.  They use the connection factory to create a connection.

  4.  They use the connection to create a session.
    
  5.  They use the session to create message producers and/or consumers for 
      the topic or queue.

Each example contains comments that provide details on what it does and how it
works.


Basic Examples
==============

The most basic JMS examples do the following:

  - SimpleQueueSender.java and SynchQueueReceiver.java send and synchronously 
    receive a single text message using a queue.
    
    If you run these programs in two different windows, the order in which you 
    start them does not matter.  If you run them in the same window, run
    SimpleQueueSender first.  Each program takes a queue name as a command-line
    argument.
    
    The output of SimpleQueueSender looks like this (the queue name is SQ):

      % java SimpleQueueSender SQ
      Queue name is SQ
      Sending message: Here is a message

    The output of SynchQueueReceiver looks like this:

      % java SynchQueueReceiver SQ
      Queue name is SQ
      Reading message: Here is a message

  - SynchTopicExample.java uses a publisher class and a subscriber class to 
    publish and synchronously receive a single text message using a topic.  The
    program takes a topic name as a command-line argument.

    The output of SynchTopicExample looks like this (the topic name is ST):

      % java SynchTopicExample ST
      Topic name is ST
      PUBLISHER THREAD: Publishing message: Here is a message
      SUBSCRIBER THREAD: Reading message: Here is a message

These examples contain more detailed explanatory comments than the others.


Intermediate Examples
=====================

The intermediate JMS examples do the following:

  - MultipleQueueSender.java and AsynchQueueReceiver.java send five text 
    messages to a queue and asynchronously receive them using a message listener 
    (TextListener), which is in the file TextListener.java.

    If you run these programs in two different windows, the order in which you 
    start them does not matter.  If you run them in the same window, run
    MultipleQueueSender first.

    Feel free to experiment by running SimpleQueueSender with 
    AsynchQueueReceiver, and by running MultipleQueueSender with 
    SynchQueueReceiver (in the second case, you need to run SynchQueueReceiver
    five times to fetch all the messages).

  - AsynchTopicExample.java uses a publisher class and a subscriber class to
    publish five text messages to a topic and asynchronously get them using a 
    message listener (TextListener).

  - MessageFormats.java writes and reads messages in the five supported message 
    formats.  The messages are not sent, so you do not need to specify a queue
    or topic argument when you run the program.
    
  - MessageConversion.java shows that for some message formats, you can write 
    a message using one data type and read it using another.  The StreamMessage 
    format allows conversion between String objects and other data types.  The
    BytesMessage format allows more limited conversions.  You do not need to 
    specify a queue or topic argument.
    
  - ObjectMessages.java shows that objects are copied into messages, not passed
    by reference: once you create a message from a given object, you can change
    the original object, but the contents of the message do not change.  You do 
    not need to specify a queue or topic argument.
    
  - BytesMessages.java shows how to write and read a BytesMessage of
    indeterminate length.  It uses a text file, but the same basic technique can
    be used with any kind of file, including a binary one.  Specify a text file
    on the command line when you run the program:
    
      % java BytesMessages <filename>


Advanced Examples
=================

The advanced examples do the following:

  - RequestReplyQueue.java uses the JMS request/reply facility, which supports 
    situations in which every message sent requires a response.  The sending 
    application can create a QueueRequestor or TopicRequestor, which 
    encapsulates the creation and use of a temporary destination where a reply 
    is sent.

  - MessageHeadersTopic.java illustrates the use of the JMS message header
    fields.  It displays the values of the header fields both before and after 
    a message is sent, and shows how the publish method sets the fields.

  - TopicSelectors.java shows how to use message header fields as message 
    selectors.  The program consists of one publisher and several subscribers. 
    Each subscriber uses a message selector to receive a subset of the messages 
    sent by the publisher.

  - DurableSubscriberExample.java shows how you can create a durable subscriber
    that retains messages published to a topic while the subscriber is inactive.


After You Finish
================

After you run the examples, you can delete the topic and queue you created and
stop the JMS service.
