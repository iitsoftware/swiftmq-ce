
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
 * PubSub example to receive requests from a topic and send replies back.
 *
 * The program expects the following optional parameters:
 * - SMQP URL to create a JNDI connection [smqp://localhost:4001]
 * - Connection Factory to lookup [plainsocket@router2]
 * - Topic name to lookup [testtopic]
 * - Number of messages to receive [1]
 *
 * You may either specify all parameters or none. If you specify none, the
 * values in brackets are used per default, thus, the program connects to
 * router2, receives a request from topic 'testtopic' and sends a reply
 * back to the requestor.
 *
 * @author IIT GmbH, Bremen/Germany
 */
public class PubSubReplier
{
  public static void main(String[] args)
  {
    String smqpURL = "smqp://localhost:4001";
    String tcfName = "plainsocket@router2";
    String topicName = "testtopic";
    int nMsgs = 1;
    if (args.length > 0 && args.length < 4)
    {
      System.out.println();
      System.out.println("Usage: java PubSubReplier <smqpURL> <tcfName> <topicName> <nReplies>\n");
      System.out.println("You may either specify all parameters or none. If you specify none, ");
      System.out.println("the following defaults are used: \n");
      System.out.println("   " + smqpURL + " " + tcfName + " " + topicName + " " + nMsgs + "\n");
      System.exit(-1);
    }
    if (args.length == 4)
    {
      smqpURL = args[0];
      tcfName = args[1];
      topicName = args[2];
      nMsgs = Integer.parseInt(args[3]);
    }
    System.out.println();
    System.out.println("The following parameters are used:\n");
    System.out.println("SMQP-URL       : " + smqpURL);
    System.out.println("TCF Name       : " + tcfName);
    System.out.println("Topic Name     : " + topicName);
    System.out.println("Number Replies : " + nMsgs);
    System.out.println();
    try
    {
      // Perform the JNDI lookup.
      Hashtable env = new Hashtable();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.swiftmq.jndi.InitialContextFactoryImpl");
      env.put(Context.PROVIDER_URL, smqpURL);
      InitialContext ctx = new InitialContext(env);
      TopicConnectionFactory connectionFactory = (TopicConnectionFactory) ctx.lookup(tcfName);
      Topic topic = (Topic) ctx.lookup(topicName);

      // Important to note that you should close the context thereafter, because
      // the context holds an active JMS connection.
      ctx.close();

      // Create connection, session & subscriber
      TopicConnection connection = connectionFactory.createTopicConnection();
      TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      TopicSubscriber subscriber = session.createSubscriber(topic);

      // Create an unidentified TopicPublisher for the replies to send.
      TopicPublisher replyPublisher = session.createPublisher(null);
      replyPublisher.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      // Start the connection
      connection.start();

      // Receive the requests and send the replies
      for (int i = 0; i < nMsgs; i++)
      {
        TextMessage msg = (TextMessage) subscriber.receive();
        String s = msg.getText();
        System.out.println(s + " received.");
        msg.clearBody();
        msg.setText("Re: " + s);
        System.out.println("Sending: " + msg.getText());
        replyPublisher.publish((Topic) msg.getJMSReplyTo(), msg);
      }

      // Close resources
      replyPublisher.close();
      subscriber.close();
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

