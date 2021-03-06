import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.v100.client.Connection;
import com.swiftmq.amqp.v100.client.DurableConsumer;
import com.swiftmq.amqp.v100.client.QoS;
import com.swiftmq.amqp.v100.client.Session;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPString;
import com.swiftmq.net.JSSESocketFactory;

/*
 * Copyright (c) 2011 IIT Software GmbH, Bremen/Germany. All Rights Reserved.
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
 * A receiver that receives AMQP messages in a non-transacted fashion and uses a durable topic subscription.
 * <p/>
 * The program expects the following optional parameters:
 * - Host name [localhost]
 * - Port [5672]
 * - Source [testtopic]
 * - Number of messages to receive [100]
 * - Quality of Service [EXACTLY_ONCE]
 * - Container Id [testcid]
 * - Durable link name [testdurable]
 * - Keep durable [false]
 * - Authenticate as anonymous [Uses SASL with anonymous login]
 * - User name [null]
 * - Password [null]
 *
 * @author IIT Software GmbH, Bremen/Germany
 */

public class ReceiverNonTransactedDurable
{
  private static int toIntQoS(String s) throws Exception
  {
    s = s.toUpperCase();
    if (s.equals("AT_LEAST_ONCE"))
      return QoS.AT_LEAST_ONCE;
    if (s.equals("AT_MOST_ONCE"))
      return QoS.AT_MOST_ONCE;
    if (s.equals("EXACTLY_ONCE"))
      return QoS.EXACTLY_ONCE;
    throw new Exception("Invalid QoS: " + s);
  }

  public static void main(String[] args)
  {
    if (args.length == 1 && args[0].equals("?"))
    {
      System.out.println();
      System.out.println("Usage: <host> <port> <source> <nmsgs> <qos> <containerid> <durablelinkname> <keep> <authanon> [<username> <password>]");
      System.out.println("       <qos> ::= AT_LEAST_ONCE | AT_MOST_ONCE | EXACTLY_ONCE");
      System.out.println("       Suppress <username> <password> and set <authanon> to false to avoid SASL.");
      System.out.println();
      System.exit(0);
    }
    String host = "localhost";
    int port = 5672;
    String source = "testtopic";
    int nMsgs = 100;
    String qosS = "EXACTLY_ONCE";
    String cid = "testcid";
    String durableName = "testdurable";
    boolean keepDurable = false;
    boolean authAnon = true;
    String user = null;
    String password = null;
    if (args.length >= 1)
      host = args[0];
    if (args.length >= 2)
      port = Integer.parseInt(args[1]);
    if (args.length >= 3)
      source = args[2];
    if (args.length >= 4)
      nMsgs = Integer.parseInt(args[3]);
    if (args.length >= 5)
      qosS = args[4];
    if (args.length >= 6)
      cid = args[5];
    if (args.length >= 7)
      durableName = args[6];
    if (args.length >= 8)
      keepDurable = Boolean.parseBoolean(args[7]);
    if (args.length >= 9)
      authAnon = Boolean.parseBoolean(args[8]);
    if (args.length >= 10)
      user = args[9];
    if (args.length >= 11)
      password = args[10];
    System.out.println();
    System.out.println("Host        : " + host);
    System.out.println("Port        : " + port);
    System.out.println("Source      : " + source);
    System.out.println("Number Msgs : " + nMsgs);
    System.out.println("QoS         : " + qosS);
    System.out.println("Container Id: " + cid);
    System.out.println("Durable Name: " + durableName);
    System.out.println("Keep Durable: " + keepDurable);
    System.out.println("Auth as Anon: " + authAnon);
    System.out.println("User        : " + user);
    System.out.println("Password    : " + password);
    System.out.println();
    try
    {
      // Create connection and connect
      AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
      Connection connection = null;
      if (args.length < 10)
        connection = new Connection(ctx, host, port, authAnon);
      else
        connection = new Connection(ctx, host, port, user, password);
      connection.setContainerId(cid);
      if (port == 5671)
      {
        System.out.println("Using SSL on port 5671");
        connection.setSocketFactory(new JSSESocketFactory());
      }
      connection.connect();

      // Create session and consumer
      Session session = connection.createSession(50, 50);
      DurableConsumer c = session.createDurableConsumer(durableName, source, 100, toIntQoS(qosS), false, null);

      // Receive messages non-transacted
      for (int i = 0; i < nMsgs; i++)
      {
        AMQPMessage msg = c.receive();
        if (msg == null)
          break;
        AmqpValue value = msg.getAmqpValue();
        System.out.println("Received: " + ((AMQPString) value.getValue()).getValue());
        if (!msg.isSettled())
          msg.accept();
      }

      // Close everything down
      Thread.sleep(2000);
      c.close();
      if (!keepDurable)
        c.unsubscribe();
      session.close();
      connection.close();
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}
