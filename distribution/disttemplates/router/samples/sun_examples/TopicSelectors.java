/*
 * @(#)TopicSelectors.java	1.2 00/06/02
 * 
 * Copyright (c) 2000 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * Sun grants you ("Licensee") a non-exclusive, royalty free, license to use,
 * modify and redistribute this software in source and binary code form,
 * provided that i) this copyright notice and license appear on all copies of
 * the software; and ii) Licensee does not utilize the software in a manner
 * which is disparaging to Sun.
 *
 * This software is provided "AS IS," without a warranty of any kind. ALL
 * EXPRESS OR IMPLIED CONDITIONS, REPRESENTATIONS AND WARRANTIES, INCLUDING ANY
 * IMPLIED WARRANTY OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE OR
 * NON-INFRINGEMENT, ARE HEREBY EXCLUDED. SUN AND ITS LICENSORS SHALL NOT BE
 * LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING
 * OR DISTRIBUTING THE SOFTWARE OR ITS DERIVATIVES. IN NO EVENT WILL SUN OR ITS
 * LICENSORS BE LIABLE FOR ANY LOST REVENUE, PROFIT OR DATA, OR FOR DIRECT,
 * INDIRECT, SPECIAL, CONSEQUENTIAL, INCIDENTAL OR PUNITIVE DAMAGES, HOWEVER
 * CAUSED AND REGARDLESS OF THE THEORY OF LIABILITY, ARISING OUT OF THE USE OF
 * OR INABILITY TO USE SOFTWARE, EVEN IF SUN HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGES.
 *
 * This software is not designed or intended for use in on-line control of
 * aircraft, air traffic, aircraft navigation or aircraft communications; or in
 * the design, construction, operation or maintenance of any nuclear
 * facility. Licensee represents and warrants that it will not use or
 * redistribute the Software for such purposes.
 */
import java.util.*;
import javax.jms.*;
import javax.naming.*;

/**
 * The TopicSelectors class demonstrates the use of multiple 
 * subscribers and message selectors.
 * <p>
 * The program contains a Publisher class, a Subscriber class with a listener 
 * class, a main method, and a method that runs the subscriber and publisher
 * threads.
 * <p>
 * The Publisher class publishes 30 messages of 6 different types, randomly
 * selected, then publishes a "Finished" message.  The program creates seven
 * instances of the Subscriber class, one for each type and one that listens 
 * for the "Finished" message.  Each subscriber instance uses a different
 * message selector to fetch messages of only one type.  The publisher
 * displays the messages it sends, and the listener displays the messages that
 * the subscribers receive.  Because all the objects run in threads, the
 * displays are interspersed when the program runs.
 * 
 * @author Kim Haase
 * @version 1.2, 06/02/00
 */
public class TopicSelectors {
	String                    topicName = null;
	final int                 ARRSIZE = 6;
	String                    messageTypes[] = 
	{"Nation/World", "Metro/Region", "Business",
		"Sports", "Living/Arts", "Opinion"};
	String                    lastMsgType = "Finished";
	int                       exitResult = 0;
	boolean                   done = false;
	
	
	/**
	 * The Publisher class publishes a number of messages.  For each, it
	 * randomly chooses a message type.  It creates a message and sets its
	 * text to a message that indicates the message type.
	 * It also sets the client property NewsType, which the Subscriber
	 * objects use as the message selector.
	 * The final message sets the NewsType to "Finished", which signals the
	 * end of the messages.
	 *
	 * @author Kim Haase
	 * @version 1.2, 06/02/00
	 */
	public class Publisher extends Thread {
		TopicConnectionFactory      topicConnectionFactory = null;
		TopicConnection             topicConnection = null;
		TopicSession                topicSession = null;
		Topic                       topic = null;
		TopicPublisher              topicPublisher = null;
		TextMessage                 message = null;
		int                         numMsgs = ARRSIZE * 5;
		String                      messageType = null;
		
		/**
		 * Chooses a message type by using the random number generator
		 * found in java.util.
		 *
		 * @return	the String representing the message type
		 */
		public String chooseType() {
			int whichMsg;
			Random rgen = new Random();
			
			whichMsg = rgen.nextInt(ARRSIZE);
			return messageTypes[whichMsg];
		}
		
		/**
		 * Runs the thread.
		 */
		public void run() {
			try {
				topicConnectionFactory = (TopicConnectionFactory) 
					SampleUtilities.jndiLookup(SampleUtilities.TOPICCONFAC);
				topicConnection = 
					topicConnectionFactory.createTopicConnection();
				topic = (Topic) SampleUtilities.jndiLookup(topicName);
			} catch (JMSException e) {
				System.out.println("Exception occurred: " + e.toString());
				System.exit(1);
			} catch (NamingException e) {
				System.out.println("JNDI lookup problem: " + e.toString());
				System.exit(1);
			}
			
			try {
				topicSession = topicConnection.createTopicSession(false, 
					Session.AUTO_ACKNOWLEDGE);
				topicPublisher = topicSession.createPublisher(topic);
				message = topicSession.createTextMessage();
				for (int i = 0; i < numMsgs; i++) {
					messageType = chooseType();
					message.setStringProperty("NewsType", messageType);
					message.setText("Item " + i + ": " + messageType);
					System.out.println("PUBLISHER THREAD: Setting message text to: " 
						+ message.getText());
					topicPublisher.publish(message);
				}
				message.setStringProperty("NewsType", lastMsgType);
				message.setText("That's all the news for today.");
				topicPublisher.publish(message);
			} catch (JMSException e) {
				System.out.println("Exception occurred: " + e.toString());
				exitResult = 1;
			} finally {
				if (topicConnection != null) {
					try {
						topicConnection.close();
					} catch (JMSException e) {
						exitResult = 1;
					}
				}
			}
		}
	}
	
	/**
	 * Each instance of the Subscriber class creates a subscriber that uses
	 * a message selector that is based on the string passed to its 
	 * constructor.
	 * It registers its message listener, then starts listening
	 * for messages.  It does not exit until the message listener sets the 
	 * variable done to true, which happens when the listener gets the last
	 * message.
	 *
	 * @author Kim Haase
	 * @version 1.2, 06/02/00
	 */
	public class Subscriber extends Thread {
		TopicConnectionFactory       topicConnectionFactory = null;
		TopicConnection              topicConnection = null;
		TopicSession                 topicSession = null;
		Topic                        topic = null;
		TopicSubscriber              topicSubscriber = null;
		TextMessage                  message = null;
		MultipleListener             multipleListener = new MultipleListener();
		String                       whatKind;
		int                          subscriberNumber;
		String                       selector = null;
		
		/**
		 * The MultipleListener class implements the MessageListener interface  
		 * by defining an onMessage method for the Subscriber class.
		 *
		 * @author Kim Haase
		 * @version 1.2, 06/02/00
		 */
		private class MultipleListener implements MessageListener {
			
			/**
			 * Displays the message text.
			 * If the value of the NewsType property is "Finished", sets the 
			 * variable done to true; the Subscriber class interprets this as
			 * a signal to end the program.
			 *
			 * @param inMessage	the incoming message
			 */
			public void onMessage(Message inMessage) {
				TextMessage               msg = (TextMessage) inMessage;
				String                    newsType;
				
				try {
					System.out.println("SUBSCRIBER " + subscriberNumber 
						+ " THREAD: Message received: " + msg.getText());
					newsType = msg.getStringProperty("NewsType");
					if (newsType.equals("Finished")) {
						done = true;
					}
				} catch(JMSException e) {
					System.out.println("Exception in onMessage(): " 
						+ e.toString());
				}
			}
		}
		
		/**
		 * Constructor.  Sets whatKind to indicate the type of
		 * message this Subscriber object will listen for; sets
		 * subscriberNumber based on Subscriber array index.
		 *
		 * @param str	a String from the messageTypes array
		 * @param num	the index of the Subscriber array
		 */
		public Subscriber(String str, int num) {
			whatKind = str;
			subscriberNumber = num + 1;
		}
		
		/**
		 * Runs the thread.
		 */
		public void run() {
			try {
				topicConnectionFactory = (TopicConnectionFactory) 
					SampleUtilities.jndiLookup("TopicConnectionFactory");
				topicConnection = 
					topicConnectionFactory.createTopicConnection();
				topic = (Topic) SampleUtilities.jndiLookup(topicName);
			} catch (JMSException e) {
				System.out.println("Exception occurred: " + e.toString());
				System.exit(1);
			} catch (NamingException e) {
				System.out.println("JNDI lookup problem: " + e.toString());
				System.exit(1);
			}
			
			try {
				topicSession = topicConnection.createTopicSession(false, 
					Session.AUTO_ACKNOWLEDGE);
				selector = new String("NewsType = '" + whatKind + "'");
				System.out.println("Creating subscriber "
					+ subscriberNumber + "; selector is \"" + selector + "\"");
				topicSubscriber = 
					topicSession.createSubscriber(topic, selector, false);
				topicSubscriber.setMessageListener(multipleListener);
				topicConnection.start();
				while (!done) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {}
				}
			} catch (JMSException e) {
				System.out.println("Exception occurred: " + e.toString());
				exitResult = 1;
			} finally {
				if (topicConnection != null) {
					try {
						topicConnection.close();
					} catch (JMSException e) {
						exitResult = 1;
					}
				}
			}   	    
		}
	}
	
	/**
	 * Creates an array of Subscriber objects, one for each message type 
	 * including the Finished type, and starts their threads.
	 * Creates a Publisher object and starts its thread.
	 * Calls the join method to wait for the threads to die.
	 */
	public void run_threads() {
		Subscriber  subscriberArray[] = new Subscriber[ARRSIZE+ 1];
		Publisher   publisher = new Publisher();
		
		for (int i = 0; i < ARRSIZE; i++) {
			subscriberArray[i] = new Subscriber(messageTypes[i], i);
			subscriberArray[i].start();
		}
		subscriberArray[ARRSIZE] = new Subscriber(lastMsgType, ARRSIZE);
		subscriberArray[ARRSIZE].start();
		// Begin SwiftMQ
		// It's necessary to wait a little time before
		// starting the publisher to guarantee the
		// subscribers are really in place...
		try {
			Thread.sleep(5000);
		} catch (Exception ignored){}
		// End SwiftMQ
		
		publisher.start();
		
		for (int i = 0; i < (ARRSIZE + 1); i++) {
			try {
				subscriberArray[i].join();
			} catch (InterruptedException e) {}
		}
		
		try {
			publisher.join();
		} catch (InterruptedException e) {}
	}
	
	/**
	 * Reads the topic name from the command line, then calls the
	 * run_threads method to execute the program threads.
	 *
	 * @param args	the topic used by the example
	 */
	public static void main(String[] args) {
		TopicSelectors          ts = new TopicSelectors();
		
		if (args.length != 1) {
			System.out.println("Usage: java TopicSelectors <topic_name>");
			System.exit(1);
		}
		
		ts.topicName = new String(args[0]);
		System.out.println("Topic name is " + ts.topicName);
		
		ts.run_threads();
		SampleUtilities.exit(ts.exitResult);
	}
}
