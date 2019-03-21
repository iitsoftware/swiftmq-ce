/*
 * @(#)AsynchTopicExample.java	1.2 00/06/02
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
 * The AsynchTopicExample class demonstrates the use of a message listener in 
 * the publish/subscribe model.  The publisher publishes several messages, and
 * the subscriber reads them asynchronously.
 * <p>
 * The program contains a MultiplePublisher class, an AsynchSubscriber class
 * with a listener class, a main method, and a method that runs the subscriber
 * and publisher threads.
 * <p>
 * Specify a topic name on the command line when you run the program.
 *
 * @author Kim Haase
 * @version 1.2, 06/02/00
 */
public class AsynchTopicExample {
	String                      topicName = null;
	int                         exitResult = 0;
	
	/**
	 * The AsynchSubscriber class fetches several messages from a topic 
	 * asynchronously, using a message listener, TextListener.
	 *
	 * @author Kim Haase
	 * @version 1.2, 06/02/00
	 */
	public class AsynchSubscriber extends Thread {
		TopicConnectionFactory       topicConnectionFactory = null;
		Topic                        topic = null;
		TopicConnection              topicConnection = null;
		TopicSession                 topicSession = null;
		TopicSubscriber              topicSubscriber = null;
		TextMessage                  message = null; 
		TextListener                 topicListener = null;
		
		/**
		 * The TextListener class implements the MessageListener interface by 
		 * defining an onMessage method for the AsynchSubscriber class.
		 *
		 * @author Kim Haase
		 * @version 1.2, 06/02/00
		 */
		private class TextListener implements MessageListener {
			
			/**
			 * Casts the message to a TextMessage and displays its text.
			 *
			 * @param message	the incoming message
			 */
			public void onMessage(Message message) {
				TextMessage   msg = (TextMessage) message;
				
				try {
					System.out.println("SUBSCRIBER THREAD: Reading message: " 
						+ msg.getText());
				} catch (JMSException e) {
					System.out.println("Exception in onMessage(): " 
						+ e.toString());
				}
			}
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
				System.out.println("Connection problem: " + e.toString());
				System.exit(1);
			} catch (NamingException e) {
				System.out.println("JNDI lookup problem: " + e.toString());
				System.exit(1);
			}
			
			/*
			 * Create session and subscriber.
			 * Register message listener (TextListener).
			 * Start message delivery; listener displays the message obtained.
			 * Wait for user to enter Q or q to close connection.
			 */
			try {
				topicSession = topicConnection.createTopicSession(false, 
					Session.AUTO_ACKNOWLEDGE);
				topicSubscriber = topicSession.createSubscriber(topic);
				topicListener = new TextListener();
				topicSubscriber.setMessageListener(topicListener);
				topicConnection.start();
				exitResult = SampleUtilities.wait_for_quit();
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
	 * The MultiplePublisher class publishes several message to a topic. 
	 *
	 * @author Kim Haase
	 * @version 1.2, 06/02/00
	 */
	public class MultiplePublisher extends Thread {
		TopicConnectionFactory       topicConnectionFactory = null;
		Topic                        topic = null;
		TopicConnection              topicConnection = null;
		TopicSession                 topicSession = null;
		TopicPublisher               topicPublisher = null;
		String                       msgText = new String("Here is a message");
		TextMessage                  message = null;
		final int                    NUMMSGS = 5;
		
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
				System.out.println("Connection problem: " + e.toString());
				System.exit(1);
			} catch (NamingException e) {
				System.out.println("JNDI lookup problem: " + e.toString());
				System.exit(1);
			}
			
			/*
			 * Create session and publisher.
			 * Create text message.
			 * Send five messages, varying text slightly.
			 * Finally, close connection.
			 */
			try {
				topicSession = topicConnection.createTopicSession(false, 
					Session.AUTO_ACKNOWLEDGE);
				topicPublisher = topicSession.createPublisher(topic);
				message = topicSession.createTextMessage();
				for (int i = 0; i < NUMMSGS; i++) {
					message.setText(msgText + " " + (i + 1));
					System.out.println("PUBLISHER THREAD: Publishing message: " 
						+ message.getText());
					topicPublisher.publish(message);
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
	 * Instantiates the subscriber and publisher classes and starts their
	 * threads.
	 * Calls the join method to wait for the threads to die.
	 */
	public void run_threads() {
		AsynchSubscriber  asynchSubscriber = new AsynchSubscriber();
		MultiplePublisher  multiplePublisher = new MultiplePublisher();
		
		asynchSubscriber.start();
		// Begin SwiftMQ
		// It's necessary to wait a little time before
		// starting the publisher to guarantee the
		// subscriber is really in place...
		try {
			Thread.sleep(3000);
		} catch (Exception ignored){}
		// End SwiftMQ
		multiplePublisher.start();
		try {
			asynchSubscriber.join();
			multiplePublisher.join();
		} catch (InterruptedException e) {}
	}
	
	/**
	 * Reads the topic name from the command line, then calls the
	 * run_threads method to execute the program threads.
	 *
	 * @param args	the topic used by the example
	 */
	public static void main(String[] args) {
		AsynchTopicExample ate = new AsynchTopicExample();
		
		if (args.length != 1) {
			System.out.println("Usage: java AsynchTopicExample <topic_name>");
			System.exit(1);
		}
		ate.topicName = new String(args[0]);
		System.out.println("Topic name is " + ate.topicName);
		
		System.out.println("To end program, enter Q or q, then <return>");
		
		ate.run_threads();
		SampleUtilities.exit(ate.exitResult);
	}
}
