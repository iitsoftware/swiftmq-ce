/*
 * @(#)RequestReplyQueue.java	1.2 00/06/02
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
 * The RequestReplyQueue class illustrates a simple implementation of a
 * request/reply message exchange.  It uses the QueueRequestor class provided
 * by JMS.  Providers and clients can create more sophisticated versions of
 * this facility.
 * <p>
 * The program contains a Request class, a Reply class, a main method, and 
 * a method that runs the subscriber and publisher threads.
 *
 * @author Kim Haase
 * @version 1.2, 06/02/00
 */
public class RequestReplyQueue {
    String                    queueName = null;
    int                       exitResult = 0;

    /**
     * The Request class represents the request half of the message exchange.  
     *
     * @author Kim Haase
     * @version 1.2, 06/02/00
     */
    public class Request extends Thread {
        QueueConnectionFactory       queueConnectionFactory = null;
        Queue                        queue = null;
        QueueConnection              queueConnection = null;
        QueueSession                 queueSession = null;
        QueueRequestor               queueRequestor = null;
        String                       msgText = new String("Here is a request");
        TextMessage                  message = null;
        TextMessage                  reply = null;
        String                       replyID;
        
        /**
         * Runs the thread.
         */
        public void run() {
            try {
                queueConnectionFactory = (QueueConnectionFactory) 
                    SampleUtilities.jndiLookup(SampleUtilities.QUEUECONFAC);
                queueConnection = 
                    queueConnectionFactory.createQueueConnection();
                queue = (Queue) SampleUtilities.jndiLookup(queueName);
            } catch (JMSException e) {
                System.out.println("Connection problem: " + e.toString());
                System.exit(1);
            } catch (NamingException e) {
                System.out.println("JNDI lookup problem: " + e.toString());
    	        System.exit(1);
            }

            /*  
             * Create a QueueRequestor.
             * Create a text message and set its text.
             * Start delivery of incoming messages.
             * Send the text message as the argument to the request method, 
             * which returns the reply message.  The request method also
             * creates a temporary queue and places it in the JMSReplyTo
             * message header field.
             * Extract and display the reply message.
             * Read the JMSCorrelationID of the reply message and confirm that 
             * it matches the JMSMessageID of the message that was sent.
             * Finally, close the connection.
             */
            try {
                queueSession = queueConnection.createQueueSession(false, 
                    Session.AUTO_ACKNOWLEDGE);
                queueRequestor = new QueueRequestor(queueSession, queue);
                message = queueSession.createTextMessage();
                message.setText(msgText);
                System.out.println("REQUEST: Sending message: " 
                    + message.getText());
                queueConnection.start();
                reply = (TextMessage) queueRequestor.request(message);
                System.out.println("REQUEST: Reply received: " 
                    + reply.getText());
                replyID = new String(reply.getJMSCorrelationID());
                if (replyID.equals(message.getJMSMessageID())) {
                    System.out.println("REQUEST: OK: Reply matches sent message");
                } else {
                    System.out.println("REQUEST: ERROR: Reply does not match sent message");
                }
            } catch (JMSException e) {
                System.out.println("Exception occurred: " + e.toString());
                exitResult = 1;
            } finally {
                if (queueConnection != null) {
                    try {
                        queueConnection.close();
                    } catch (JMSException e) {
                        exitResult = 1;
                    }
                }
            }
        }
    }

    /**
     * The Reply class represents the reply half of the message exchange.
     *
     * @author Kim Haase
     * @version 1.2, 06/02/00
     */
    public class Reply extends Thread {
        QueueConnectionFactory       queueConnectionFactory = null;
        Queue                        queue = null;
        QueueConnection              queueConnection = null;
        QueueSession                 queueSession = null;
        QueueReceiver                queueReceiver = null;
        TextMessage                  message = null; 
        TextMessage                  reply = null; 
        String                       replyText = new String("Here is a reply");
        TemporaryQueue               tempQueue = null;
        QueueSender                  replySender = null;

        /**
         * Runs the thread.
         */
        public void run() {
            try {
                queueConnectionFactory = (QueueConnectionFactory) 
                    SampleUtilities.jndiLookup(SampleUtilities.QUEUECONFAC);
                queueConnection = 
                    queueConnectionFactory.createQueueConnection();
                queue = (Queue) SampleUtilities.jndiLookup(queueName);
            } catch (JMSException e) {
                System.out.println("Connection problem: " + e.toString());
                System.exit(1);
            } catch (NamingException e) {
                System.out.println("JNDI lookup problem: " + e.toString());
    	        System.exit(1);
            }

            /* 
             * Create a QueueReceiver.
             * Start delivery of incoming messages.
             * Call receive, which blocks until it obtains a message.
             * Display the message obtained.
             * Extract the temporary reply queue from the JMSReplyTo field of 
             * the message header.
             * Use the temporary queue to create a sender for the reply message.
             * Create the reply message, setting the JMSCorrelationID to the 
             * value of the incoming message's JMSMessageID.
             * Send the reply message.
             * Finally, close the connection. 
             */    
            try {
                queueSession = queueConnection.createQueueSession(false, 
                    Session.AUTO_ACKNOWLEDGE);
                queueReceiver = queueSession.createReceiver(queue);
                queueConnection.start();
                message = (TextMessage) queueReceiver.receive();
                System.out.println("REPLY: Message received: " + message.getText());
                tempQueue = (TemporaryQueue) message.getJMSReplyTo();
                replySender = queueSession.createSender(tempQueue);
                reply = queueSession.createTextMessage();
                reply.setText(replyText);
                reply.setJMSCorrelationID(message.getJMSMessageID());
                System.out.println("REPLY: Sending reply: " + reply.getText());
                replySender.send(reply);
            } catch (JMSException e) {
                System.out.println("Exception occurred: " + e.toString());
                exitResult = 1;
            } finally {
                if (queueConnection != null) {
                    try {
                        queueConnection.close();
                    } catch (JMSException e) {
                        exitResult = 1;
                    }
                }
            }
        }
    }
        
    /**
     * Instantiates the Request and Reply classes and starts their
     * threads.
     * Calls the join method to wait for the threads to die.
     */
    public void run_threads() {
        Request  request = new Request();
        Reply    reply = new Reply();

        request.start();
        reply.start();
        try {
            request.join();
            reply.join();
        } catch (InterruptedException e) {}
    }

    /**
     * Reads the queue name from the command line, then calls the
     * run_threads method to execute the program threads.
     *
     * @param args	the queue used by the example
     */
    public static void main(String[] args) {
        RequestReplyQueue rrq = new RequestReplyQueue();
        
        if (args.length != 1) {
    	    System.out.println("Usage: java RequestReplyQueue <queue_name>");
    	    System.exit(1);
    	}
    	
        rrq.queueName = new String(args[0]);
        System.out.println("Queue name is " + rrq.queueName);

    	rrq.run_threads();
    	SampleUtilities.exit(rrq.exitResult);
    }
}
