/*
 * @(#)SynchQueueReceiver.java	1.2 00/06/02
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
 * The SynchQueueReceiver class consists only of a main method, which fetches 
 * a single message from a queue using synchronous message delivery.  Run this 
 * program in conjunction with SimpleQueueSender.  Specify a queue name on the
 * command line when you run the program.
 * <p>
 * The program calls methods in the SampleUtilities class.
 *
 * @author Kim Haase
 * @version 1.2, 06/02/00
 */
public class SynchQueueReceiver {

    /**
     * Main method.
     *
     * @param args	the queue used by the example
     */
    public static void main(String[] args) {
        String                       queueName = null;
        QueueConnectionFactory       queueConnectionFactory = null;
        Queue                        queue = null;
        QueueConnection              queueConnection = null;
        QueueSession                 queueSession = null;
        QueueReceiver                queueReceiver = null;
        TextMessage                  message = null;
        int                          exitResult = 0;
                
    	/*
    	 * Read queue name from command line and display it.
    	 * Queue must have been created by jmsadmin tool.
    	 */
    	if (args.length != 1) {
    	    System.out.println("Usage: java SynchQueueReceiver <queue_name>");
    	    System.exit(1);
    	}
    	queueName = new String(args[0]);
    	System.out.println("Queue name is " + queueName);
    	    
        /*
         * Use JNDI to look up connection factory.
         * Create connection.
         * Look up queue name.
         */
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
         * Create session from connection; false means session is not
         * transacted.
         * Create receiver, then start message delivery.
         * Wait for text message to arrive, then display its contents.  (The
         * receive method blocks until it obtains a message.)
         * Close connection and exit.
         */
        try {
            queueSession = queueConnection.createQueueSession(false, 
                Session.AUTO_ACKNOWLEDGE);
            queueReceiver = queueSession.createReceiver(queue);
            queueConnection.start();
            message = (TextMessage) queueReceiver.receive();
            System.out.println("Reading message: " + message.getText());
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
    	SampleUtilities.exit(exitResult);
    }
}
