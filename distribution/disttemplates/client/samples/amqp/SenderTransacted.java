import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.v100.client.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
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
 * A sender that sends AMQP messages in a transacted fashion.
 * <p/>
 * The program expects the following optional parameters:
 * - Host name [localhost]
 * - Port [5672]
 * - Target [testqueue]
 * - Number of messages to send [100]
 * - Quality of Service [EXACTLY_ONCE]
 * - Number of messages per transaction [10]
 * - Authenticate as anonymous [Uses SASL with anonymous login]
 * - User name [null]
 * - Password [null]
 *
 * @author IIT Software GmbH, Bremen/Germany
 */

public class SenderTransacted
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
      System.out.println("Usage: <host> <port> <target> <nmsgs> <qos> <txsize> <authanon> [<username> <password>]");
      System.out.println("       <qos> ::= AT_LEAST_ONCE | AT_MOST_ONCE | EXACTLY_ONCE");
      System.out.println("       Suppress <username> <password> and set <authanon> to false to avoid SASL.");
      System.out.println();
      System.exit(0);
    }
    String host = "localhost";
    int port = 5672;
    String target = "testqueue";
    int nMsgs = 100;
    String qosS = "EXACTLY_ONCE";
    int txSize = 10;
    boolean authAnon = true;
    String user = null;
    String password = null;
    if (args.length >= 1)
      host = args[0];
    if (args.length >= 2)
      port = Integer.parseInt(args[1]);
    if (args.length >= 3)
      target = args[2];
    if (args.length >= 4)
      nMsgs = Integer.parseInt(args[3]);
    if (args.length >= 5)
      qosS = args[4];
    if (args.length >= 6)
      txSize = Integer.parseInt(args[5]);
    if (args.length >= 7)
      authAnon = Boolean.parseBoolean(args[6]);
    if (args.length >= 8)
      user = args[7];
    if (args.length >= 9)
      password = args[8];
    System.out.println();
    System.out.println("Host        : " + host);
    System.out.println("Port        : " + port);
    System.out.println("Target      : " + target);
    System.out.println("Number Msgs : " + nMsgs);
    System.out.println("QoS         : " + qosS);
    System.out.println("Tx Size     : " + txSize);
    System.out.println("Auth as Anon: " + authAnon);
    System.out.println("User        : " + user);
    System.out.println("Password    : " + password);
    System.out.println();
    try
    {
      // Create connection and connect
      AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
      Connection connection = null;
      if (args.length < 8)
        connection = new Connection(ctx, host, port, authAnon);
      else
        connection = new Connection(ctx, host, port, user, password);
      if (port == 5671)
      {
        System.out.println("Using SSL on port 5671");
        connection.setSocketFactory(new JSSESocketFactory());
      }
      connection.connect();

      // Create session and producer
      Session session = connection.createSession(50, 50);
      Producer p = session.createProducer(target, toIntQoS(qosS));

      // Get the transaction controller
      TransactionController txc = session.getTransactionController();

      // Send messages in transactions in size <txSize>
      int currentTxSize = 0;
      TxnIdIF txnId = txc.createTxnId();
      for (int i = 0; i < nMsgs; i++)
      {
        AMQPMessage msg = new AMQPMessage();
        String s = "Message #" + (i + 1);
        System.out.println("Sending " + s);
        msg.setAmqpValue(new AmqpValue(new AMQPString(s)));
        msg.setTxnIdIF(txnId);
        p.send(msg);
        currentTxSize++;
        if ((i + 1) % txSize == 0)
        {
          txc.commit(txnId);
          txnId = txc.createTxnId();
          currentTxSize = 0;
        }
      }
      if (currentTxSize > 0)
        txc.commit(txnId);

      // Close everything down
      Thread.sleep(2000);
      p.close();
      session.close();
      connection.close();
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}
