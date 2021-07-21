
import javax.jms.*;
import javax.naming.*;
import java.util.Hashtable;

/*
 * Copyright (c) 2001 IIT GmbH, Bremen/Germany. All Rights Reserved.
 *
 * IIT grants you ("Licensee") a non-exclusive, royalty free, license to use,
 * modify and redistribute this software in source and binary code form,
 * provided that i) this copyright notice and license appear on all copies of
 * the software; and ii) Licensee does not utilize the software in a manner
 * which is disparaging to IIT.
 *
 * This software is provided "AS IS," without a warranty of any kind. ALL
 * EXPRESS OR IMPLIED CONDITIONS, REPRESENTATIONS AND WARRANTIES, INCLUDING ANY
 * IMPLIED WARRANTY OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE OR
 * NON-INFRINGEMENT, ARE HEREBY EXCLUDED. IIT AND ITS LICENSORS SHALL NOT BE
 * LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING
 * OR DISTRIBUTING THE SOFTWARE OR ITS DERIVATIVES. IN NO EVENT WILL IIT OR ITS
 * LICENSORS BE LIABLE FOR ANY LOST REVENUE, PROFIT OR DATA, OR FOR DIRECT,
 * INDIRECT, SPECIAL, CONSEQUENTIAL, INCIDENTAL OR PUNITIVE DAMAGES, HOWEVER
 * CAUSED AND REGARDLESS OF THE THEORY OF LIABILITY, ARISING OUT OF THE USE OF
 * OR INABILITY TO USE SOFTWARE, EVEN IF IIT HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGES.
 *
 * This software is not designed or intended for use in on-line control of
 * aircraft, air traffic, aircraft navigation or aircraft communications; or in
 * the design, construction, operation or maintenance of any nuclear
 * facility. Licensee represents and warrants that it will not use or
 * redistribute the Software for such purposes.
 */

/**
 * P2P example to receive requests from a local queue and send replies back.
 *
 * The program expects the following optional parameters:
 * - SMQP URL to create a JNDI connection [smqp://localhost:4001]
 * - Connection Factory to lookup [plainsocket@router2]
 * - Queue name to lookup [testqueue@router2]
 * - Number of messages to receive [1]
 *
 * You may either specify all parameters or none. If you specify none, the
 * values in brackets are used per default, thus, the program connects to
 * router2, receives a request from queue 'testqueue@router2' and sends a reply
 * back to the requestor.
 *
 * @author IIT GmbH, Bremen/Germany
 */
public class P2PReplier
{
  public static void main(String[] args)
  {
    String smqpURL = "smqp://localhost:4001";
    String qcfName = "plainsocket@router2";
    String queueName = "testqueue@router2";
    int nMsgs = 1;
    if (args.length > 0 && args.length < 4)
    {
      System.out.println();
      System.out.println("Usage: java P2PReplier <smqpURL> <qcfName> <queueName> <nReplies>\n");
      System.out.println("You may either specify all parameters or none. If you specify none, ");
      System.out.println("the following defaults are used: \n");
      System.out.println("   " + smqpURL + " " + qcfName + " " + queueName + " " + nMsgs + "\n");
      System.exit(-1);
    }
    if (args.length == 4)
    {
      smqpURL = args[0];
      qcfName = args[1];
      queueName = args[2];
      nMsgs = Integer.parseInt(args[3]);
    }
    System.out.println();
    System.out.println("The following parameters are used:\n");
    System.out.println("SMQP-URL       : " + smqpURL);
    System.out.println("QCF Name       : " + qcfName);
    System.out.println("Queue Name     : " + queueName);
    System.out.println("Number Replies : " + nMsgs);
    System.out.println();
    try
    {
      // Perform the JNDI lookup.
      Hashtable env = new Hashtable();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.swiftmq.jndi.InitialContextFactoryImpl");
      env.put(Context.PROVIDER_URL, smqpURL);
      InitialContext ctx = new InitialContext(env);
      QueueConnectionFactory connectionFactory = (QueueConnectionFactory) ctx.lookup(qcfName);
      javax.jms.Queue queue = (javax.jms.Queue) ctx.lookup(queueName);

      // Important to note that you should close the context thereafter, because
      // the context holds an active JMS connection.
      ctx.close();

      // Create connection, session & receiver
      QueueConnection connection = connectionFactory.createQueueConnection();
      QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      QueueReceiver receiver = session.createReceiver(queue);

      // Create an unidentified QueueSender for the replies to send.
      QueueSender replySender = session.createSender(null);
      replySender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      // Start the connection
      connection.start();

      // Receive the requests and send the replies
      for (int i = 0; i < nMsgs; i++)
      {
        TextMessage msg = (TextMessage) receiver.receive();
        String s = msg.getText();
        System.out.println(s + " received.");
        msg.clearBody();
        msg.setText("Re: " + s);
        System.out.println("Sending: " + msg.getText());
        replySender.send((Queue) msg.getJMSReplyTo(), msg);
      }

      // Close resources
      replySender.close();
      receiver.close();
      session.close();
      connection.close();

      System.out.println("\nFinished.");

    } catch (Exception e)
    {
      System.err.println("Exception: " + e);
      System.exit(-1);
    }
  }
}

