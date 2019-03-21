import javax.jms.*;
import javax.jms.Queue;
import javax.naming.*;
import java.util.*;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.FileReader;

/*
 * Copyright (c) 2010 IIT Software GmbH, Bremen/Germany. All Rights Reserved.
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
 * Example to use the CLI Message Interface
 *
 * The program expects the following optional parameters:
 * - SMQP URL to create a JNDI connection
 * - Connection Factory to lookup
 * - Queue name to lookup
 * - Filename containing CLI commands
 *
 * @author IIT Software GmbH, Bremen/Germany
 */
public class CLIMessageInterface
{

  private static String getFileContent(String filename) throws Exception
  {
    BufferedReader reader = new BufferedReader(new FileReader(filename));
    StringBuffer b = new StringBuffer();
    String s;
    while ((s = reader.readLine())!= null)
    {
      if (s.trim().length() > 0 && !s.startsWith("#"))
        b.append(s+"\n");
    }
    return b.toString();
  }

	public static void main(String[] args)
	{
		String smqpURL = "smqp://localhost:4001";
		String qcfName = "plainsocket@router1";
		String queueName = "swiftmqmgmt-message-interface";
		String filename = null;
		if (args.length != 4)
		{
			System.out.println();
			System.out.println("Usage: java CLIMessageInterface <smqpURL> <qcfName> <queueName> <CLI filename>\n");
      System.out.println();
			System.exit(-1);
		}
    smqpURL = args[0];
    qcfName = args[1];
    queueName = args[2];
    filename = args[3];
		System.out.println();
		System.out.println("The following parameters are used:\n");
		System.out.println("SMQP-URL       : "+smqpURL);
		System.out.println("QCF Name       : "+qcfName);
		System.out.println("Queue Name     : "+queueName);
		System.out.println("CLI Filename   : "+filename);
		System.out.println();
		try {
			// Perform the JNDI lookup.
			Hashtable env = new Hashtable();
			env.put(Context.INITIAL_CONTEXT_FACTORY,"com.swiftmq.jndi.InitialContextFactoryImpl");
			env.put(Context.PROVIDER_URL,smqpURL);
			InitialContext ctx = new InitialContext(env);
			QueueConnectionFactory connectionFactory = (QueueConnectionFactory)ctx.lookup(qcfName);
			Queue queue = (Queue)ctx.lookup(queueName);
			
			// Important to note that you should close the context thereafter, because
			// the context holds an active JMS connection.
			ctx.close();
			
			// Create connection, session & sender
			QueueConnection connection = connectionFactory.createQueueConnection();
			QueueSession session = connection.createQueueSession(false,Session.AUTO_ACKNOWLEDGE);
			QueueSender sender = session.createSender(queue);
			sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			
			TemporaryQueue replyQueue = session.createTemporaryQueue();
			QueueReceiver replyReceiver = session.createReceiver(replyQueue);
			
			// Start the connection
			connection.start();
			
			TextMessage msg = session.createTextMessage();
			msg.setJMSReplyTo(replyQueue);
      msg.setText(getFileContent(filename));
      sender.send(msg);
      TextMessage reply = (TextMessage)replyReceiver.receive();
      System.out.println("\nR E S U L T :\n\n"+reply.getText());

			// Close resources
			replyQueue.delete();
			replyReceiver.close();
			sender.close();
			session.close();
			connection.close();
			
		} catch (Exception e)
		{
			System.err.println("Exception: "+e);
			System.exit(-1);
		}
	}
}

