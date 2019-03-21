/*
 * @(#)MessageHeadersTopic.java	1.2 00/06/02
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
import java.sql.*;
import java.util.*;
import javax.jms.*;
import javax.naming.*;

/**
 * The MessageHeadersTopic class demonstrates the use of message header fields.
 * <p>
 * The program contains a HeaderPublisher class, a HeaderSubscriber class, a
 * display_headers() method that is called by both classes, a main method, and 
 * a method that runs the subscriber and publisher threads.
 * <p>
 * The publishing class sends three messages, and the subscribing class
 * receives them.  The program displays the message headers just before the 
 * publish call and just after the receive so that you can see which ones are
 * set by the publish method.
 * <p>
 * Specify a topic name on the command line when you run the program.
 *
 * @author Kim Haase
 * @version 1.2, 06/02/00
 */
public class MessageHeadersTopic {
	String                       topicName = null;
	int                          exitResult = 0;
	
	/**
	 * The HeaderPublisher class publishes three messages, setting the JMSType  
	 * message header field, one of three header fields that are not set by 
	 * the publish method.  (The others, JMSCorrelationID and JMSReplyTo, are 
	 * demonstrated in the RequestReplyQueue example.)  It also sets a 
	 * client property, "messageNumber". 
	 *
	 * The displayHeaders method is called just before the publish method.
	 *
	 * @author Kim Haase
	 * @version 1.2, 06/02/00
	 */
	public class HeaderPublisher extends Thread {
		TopicConnectionFactory      topicConnectionFactory = null;
		TopicConnection             topicConnection = null;
		TopicSession                topicSession = null;
		Topic                       topic = null;
		TopicPublisher              topicPublisher = null;
		TextMessage                 message = null;
		String                      messageText = new String("Read My Headers");
		
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
				
				/* 
				 * First message: no-argument form of publish method
				 */
				message = topicSession.createTextMessage();
				message.setJMSType("Simple");
				System.out.println("PUBLISHER THREAD: Setting JMSType to " 
					+ message.getJMSType());
				message.setIntProperty("messageNumber", 1);
				System.out.println("PUBLISHER THREAD: Setting client property messageNumber to " 
					+ message.getIntProperty("messageNumber"));
				message.setText(messageText);
				System.out.println("PUBLISHER THREAD: Setting message text to: " 
					+ message.getText());
				System.out.println("Headers before message is sent:");
				displayHeaders(message);
				topicPublisher.publish(message);
				
				/* 
				 * Second message: 3-argument form of publish method;
				 * explicit setting of delivery mode, priority, and
				 * expiration
				 */
				message = topicSession.createTextMessage();
				message.setJMSType("Less Simple");
				System.out.println("\nPUBLISHER THREAD: Setting JMSType to " 
					+ message.getJMSType());
				message.setIntProperty("messageNumber", 2);
				System.out.println("PUBLISHER THREAD: Setting client property messageNumber to " 
					+ message.getIntProperty("messageNumber"));
				message.setText(messageText + " Again");
				System.out.println("PUBLISHER THREAD: Setting message text to: " 
					+ message.getText());
				displayHeaders(message);
				topicPublisher.publish(message, DeliveryMode.NON_PERSISTENT,
					3, 10000);
				
				/* 
				 * Third message: no-argument form of publish method,
				 * MessageID and Timestamp disabled
				 */
				message = topicSession.createTextMessage();
				message.setJMSType("Disable Test");
				System.out.println("\nPUBLISHER THREAD: Setting JMSType to " 
					+ message.getJMSType());
				message.setIntProperty("messageNumber", 3);
				System.out.println("PUBLISHER THREAD: Setting client property messageNumber to " 
					+ message.getIntProperty("messageNumber"));
				message.setText(messageText + " with MessageID and Timestamp disabled");
				System.out.println("PUBLISHER THREAD: Setting message text to: " 
					+ message.getText());
				topicPublisher.setDisableMessageID(true);
				topicPublisher.setDisableMessageTimestamp(true);
				System.out.println("PUBLISHER THREAD: Disabling Message ID and Timestamp");
				displayHeaders(message);
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
	 * The HeaderSubscriber class receives the three messages and calls the
	 * displayHeaders method to show how the publish method changed the
	 * header values.
	 * <p>
	 * The first message, in which no fields were set explicitly by the publish 
	 * method, shows the default values of these fields.
	 * <p>
	 * The second message shows the values set explicitly by the publish method.
	 * <p>
	 * The third message shows whether disabling the MessageID and Timestamp 
	 * has any effect in the current JMS implementation.
	 *
	 * @author Kim Haase
	 * @version 1.2, 06/02/00
	 */
	public class HeaderSubscriber extends Thread {
		TopicConnectionFactory       topicConnectionFactory = null;
		TopicConnection              topicConnection = null;
		TopicSession                 topicSession = null;
		Topic                        topic = null;
		TopicSubscriber              topicSubscriber = null;
		TextMessage                  message = null;
		
		/**
		 * Runs the thread.
		 */
		public void run() {
			try {
				topicConnectionFactory = (TopicConnectionFactory) 
					SampleUtilities.jndiLookup("TopicConnectionFactory");
				/* SwiftMQ: It's even better to make an anonymous login,
				   otherwise the user 'guest' must be created before
					 running the examples...
				topicConnection = 
					topicConnectionFactory.createTopicConnection("guest", 
					"guest");
				*/
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
				topicSubscriber = topicSession.createSubscriber(topic);
				topicConnection.start();
				for (int i = 0; i < 3; i++) {
					message = (TextMessage) topicSubscriber.receive();
					System.out.println("\nSUBSCRIBER THREAD: Message received: " 
						+ message.getText());
					System.out.println("SUBSCRIBER THREAD: Headers after message is "
						+ "received:");
					displayHeaders(message);
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
	 * Displays all message headers.
	 *
	 * @param message	the message whose headers are to be displayed
	 */
	public void displayHeaders (Message message) {
		int                          delMode = 0;
		long                         expiration = 0;
		Time                         expTime = null;
		long                         timestamp = 0;
		Time                         timestampTime = null;
		String                       propertyName = null;
		
		try {
			System.out.println("Headers set by publish/send method: ");
			
			/*
			 * Display the destination (topic, in this case).
			 */
			System.out.println(" JMSDestination: " 
				+ message.getJMSDestination());
			
			/*
			 * Display the delivery mode.  Draws an exception if not set.
			 */
			try {
				delMode = message.getJMSDeliveryMode();
				if (delMode == DeliveryMode.NON_PERSISTENT) {
					System.out.println(" JMSDeliveryMode: non-persistent");
				} else if (delMode == DeliveryMode.PERSISTENT) {
					System.out.println(" JMSDeliveryMode: persistent");
				} else {
					System.out.println(" JMSDeliveryMode: neither persistent "
						+ "nor non-persistent; error");
				}
			} catch (JMSException e) {
				System.out.println("Exception occurred: " + e.toString());
				exitResult = 1;
			}
			
			/*
			 * Display the expiration time.  If value is 0 (the default),
			 * the message never expires.  Otherwise, cast the value
			 * to a Time object for display.
			 */
			expiration = message.getJMSExpiration();
			if (expiration != 0) {
				expTime = new Time(expiration);
				System.out.println(" JMSExpiration: " + expTime);
			} else {
				System.out.println(" JMSExpiration: " + expiration);
			}
			
			/*
			 * Display the priority (default is 4).
			 */
			System.out.println(" JMSPriority: " + message.getJMSPriority());
			
			/*
			 * Display the message ID.
			 */
			System.out.println(" JMSMessageID: " + message.getJMSMessageID());
			
			/*
			 * Display the timestamp.  Draws an exception if not set.
			 * If value is not 0, cast it to a Time object for display.
			 */
			try {
				timestamp = message.getJMSTimestamp();
				if (timestamp != 0) {
					timestampTime = new Time(timestamp);
					System.out.println(" JMSTimestamp: " + timestampTime);
				} else {
					System.out.println(" JMSTimestamp: " + timestamp);
				}
			} catch (JMSException e) {
				System.out.println("Exception occurred: " + e.toString());
				exitResult = 1;
			}
			
			/*
			 * Display the correlation ID.
			 */
			System.out.println(" JMSCorrelationID: "
				+ message.getJMSCorrelationID());
			
			/*
			 * Display the ReplyTo destination.
			 */
			System.out.println(" JMSReplyTo: " + message.getJMSReplyTo());
			
			/*
			 * Display the Redelivered value (usually false).
			 */
			System.out.println("Header set by JMS provider:");
			System.out.println(" JMSRedelivered: " + message.getJMSRedelivered());
			
			/*
			 * Display the JMSType.
			 */
			System.out.println("Headers set by client program:");
			System.out.println(" JMSType: " + message.getJMSType());
			
			/*
			 * Display any client properties.
			 */
			for (Enumeration e = message.getPropertyNames(); e.hasMoreElements() ;) {
				propertyName = new String((String) e.nextElement());
				System.out.println(" Client property " + propertyName 
					+ ": " + message.getObjectProperty(propertyName)); 
			}
		} catch (JMSException e) {
			System.out.println("Exception occurred: " + e.toString());
			exitResult = 1;
		}
	}
	
	/**
	 * Instantiates the subscriber and publisher classes and starts their
	 * threads.
	 * Calls the join method to wait for the threads to die.
	 */
	public void run_threads() {
		HeaderSubscriber  headerSubscriber = new HeaderSubscriber();
		HeaderPublisher   headerPublisher = new HeaderPublisher();
		
		headerSubscriber.start();
		// Begin SwiftMQ
		// It's necessary to wait a little time before
		// starting the publisher to guarantee the
		// subscriber is really in place...
		try {
			Thread.sleep(3000);
		} catch (Exception ignored){}
		// End SwiftMQ
		headerPublisher.start();
		try {
			headerSubscriber.join();
			headerPublisher.join();
		} catch (InterruptedException e) {}
	}
	
	/**
	 * Reads the topic name from the command line, then calls the
	 * run_threads method to execute the program threads.
	 *
	 * @param args	the topic used by the example
	 */
	public static void main(String[] args) {
		MessageHeadersTopic               mht = new MessageHeadersTopic();
		
		if (args.length != 1) {
			System.out.println("Usage: java MessageHeadersTopic <topic_name>");
			System.exit(1);
		}
		mht.topicName = new String(args[0]);
		System.out.println("Topic name is " + mht.topicName);
		
		mht.run_threads();
		SampleUtilities.exit(mht.exitResult); 
	}
}
