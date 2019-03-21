/*
 * @(#)SynchTopicExample.java	1.2 00/06/02
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
import javax.jms.*;
import javax.naming.*;

/**
 * The SynchTopicExample class demonstrates the simplest form of the 
 * publish/subscribe model: the publisher publishes a message, and the 
 * subscriber reads it using a synchronous receive.
 * <p>
 * The program contains a SimplePublisher class, a SynchSubscriber class, a
 * main method, and a method that runs the subscriber and publisher
 * threads.
 * <p>
 * Specify a topic name on the command line when you run the program.
 * <p>
 * The program calls methods in the SampleUtilities class.
 *
 * @author Kim Haase
 * @version 1.2, 06/02/00
 */
public class SynchTopicExample {
	String                      topicName = null;
	int                         exitResult = 0;
	
	/**
	 * The SynchSubscriber class fetches a single message from a topic using 
	 * synchronous message delivery.
	 *
	 * @author Kim Haase
	 * @version 1.2, 06/02/00
	 */
	public class SynchSubscriber extends Thread {
		TopicConnectionFactory       topicConnectionFactory = null;
		TopicConnection              topicConnection = null;
		Topic                        topic = null;
		TopicSession                 topicSession = null;
		TopicSubscriber              topicSubscriber = null;
		TextMessage                  message = null; 
		
		/**
		 * Runs the thread.
		 */
		public void run() {
			
			/*
			 * Use JNDI to look up connection factory.
			 * Create a connection.
			 * Look up topic name.
			 */
			try {
				topicConnectionFactory = (TopicConnectionFactory) 
					SampleUtilities.jndiLookup(SampleUtilities.TOPICCONFAC);
				topicConnection = 
					topicConnectionFactory.createTopicConnection();
				topic = (Topic) SampleUtilities.jndiLookup(topicName);
			} catch (JMSException e) {
				System.out.println("Connection problem: " + e.toString());
				System.exit(1);
			} catch (NamingException e) {
				System.out.println("JNDI lookup problem: " + e.toString());
				System.exit(1);
			}
			
			/*
			 * Create session from connection; false means session is not
			 * transacted.
			 * Create subscriber, then start message delivery.
			 * Wait for text message to arrive, then display its contents.
			 * Close connection and exit.
			 */
			try {
				topicSession = topicConnection.createTopicSession(false, 
					Session.AUTO_ACKNOWLEDGE);
				topicSubscriber = topicSession.createSubscriber(topic);
				topicConnection.start();
				message = (TextMessage) topicSubscriber.receive();
				System.out.println("SUBSCRIBER THREAD: Reading message: " 
					+ message.getText());
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
	 * The SimplePublisher class publishes a single message to a topic. 
	 *
	 * @author Kim Haase
	 * @version 1.2, 06/02/00
	 */
	public class SimplePublisher extends Thread {
		TopicConnectionFactory       topicConnectionFactory = null;
		Topic                        topic = null;
		TopicConnection              topicConnection = null;
		TopicSession                 topicSession = null;
		TopicPublisher               topicPublisher = null;
		String                       msgText = new String("Here is a message");
		TextMessage                  message = null;
		
		/**
		 * Runs the thread.
		 */
		public void run() {
			
			/*
			 * Use JNDI to look up connection factory.
			 * Create connection.
			 * Look up topic name.
			 */
			try {
				topicConnectionFactory = (TopicConnectionFactory) 
					SampleUtilities.jndiLookup(SampleUtilities.TOPICCONFAC);
				topicConnection = 
					topicConnectionFactory.createTopicConnection();
				topic = (Topic) SampleUtilities.jndiLookup(topicName);
			} catch (JMSException e) {
				System.out.println("Connection problem: " + e.toString());
				System.exit(1);
			} catch (NamingException e) {
				System.out.println("JNDI lookup problem: " + e.toString());
				System.exit(1);
			}
			
			/*
			 * Create session from connection; false means session is not
			 * transacted.
			 * Create publisher and text message.
			 * Set message text, display it, and publish message.
			 * Close connection and exit.
			 */
			try {
				topicSession = topicConnection.createTopicSession(false, 
					Session.AUTO_ACKNOWLEDGE);
				topicPublisher = topicSession.createPublisher(topic);
				message = topicSession.createTextMessage();
				message.setText(msgText);
				System.out.println("PUBLISHER THREAD: Publishing message: " 
					+ message.getText());
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
	 * Instantiates the subscriber and publisher classes and starts their
	 * threads.
	 * Calls the join method to wait for the threads to die.
	 * <p>
	 * It is essential to start the subscriber before starting the publisher.
	 * In the publish/subscribe model, a subscriber can ordinarily receive only 
	 * messages published while it is active.
	 */
	public void run_threads() {
		SynchSubscriber  synchSubscriber = new SynchSubscriber();
		SimplePublisher  simplePublisher = new SimplePublisher();
		
		synchSubscriber.start();
		// Begin SwiftMQ
		// It's necessary to wait a little time before
		// starting the publisher to guarantee the
		// subscriber is really in place...
		try {
			Thread.sleep(3000);
		} catch (Exception ignored){}
		// End SwiftMQ
		simplePublisher.start();
		try {
			synchSubscriber.join();
			simplePublisher.join();
		} catch (InterruptedException e) {}
	}
	
	/**
	 * Reads the topic name from the command line and displays it.  The
	 * topic must have been created by the jmsadmin tool.
	 * Calls the run_threads method to execute the program threads.
	 * Exits program.
	 *
	 * @param args	the topic used by the example
	 */
	public static void main(String[] args) {
		SynchTopicExample ste = new SynchTopicExample();
		
		if (args.length != 1) {
			System.out.println("Usage: java SynchTopicExample <topic_name>");
			System.exit(1);
		}
		ste.topicName = new String(args[0]);
		System.out.println("Topic name is " + ste.topicName);
		
		ste.run_threads();
		SampleUtilities.exit(ste.exitResult);
	}
}
