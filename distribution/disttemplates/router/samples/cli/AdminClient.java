import javax.jms.*;
import javax.naming.*;
import java.util.*;
import java.io.*;
import com.swiftmq.admin.cli.*;
import com.swiftmq.admin.cli.event.*;

/*
 * Copyright (c) 2000 IIT GmbH, Bremen/Germany. All Rights Reserved.
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
 * The AdminClient is an example for using SwiftMQ's CLI Admin API. It reads
 * commands from System.in and executes them via CLI's executeCommand method.
 *
 * @author IIT GmbH, Bremen/Germany
 */

public class AdminClient
{
	static BufferedReader inReader = new BufferedReader(new InputStreamReader(System.in));
	
	public static String concat(String[] s, String delimiter)
	{
		StringBuffer b = new StringBuffer();
		for (int i=0;i<s.length;i++)
		{
			if (i>0)
				b.append(delimiter);
			b.append(s[i]);
		}
		return b.toString();
	}
	
	private static String readLine()
	{
		String line = null;
		boolean ok = false;
		try {
			while (!ok)
			{
				if (line == null)
					line = inReader.readLine();
				ok = line != null && !line.trim().startsWith("#") || line == null;
			}
		} catch (Exception e){}
		return line;
	}

	public static void main(String[] args)
	{
		QueueConnection connection = null;
		CLI cli = null;
		try {
			Hashtable env = new Hashtable();
			env.put(Context.INITIAL_CONTEXT_FACTORY,"com.swiftmq.jndi.InitialContextFactoryImpl");
			env.put(Context.PROVIDER_URL,"smqp://localhost:4001/timeout=10000");
			InitialContext ctx = new InitialContext(env);
			QueueConnectionFactory connectionFactory = (QueueConnectionFactory)ctx.lookup("QueueConnectionFactory");
			connection = connectionFactory.createQueueConnection();
			ctx.close();
			cli = new CLI(connection);
		} catch (Exception e)
		{
			System.out.println(e);
			System.exit(-1);
		}
		
		cli.addRouterListener(new RouterListener()
		{
			public void onRouterEvent(String routerName, boolean available)
			{
				System.out.println("Router '"+routerName+"' is "+(available?"AVAILABLE":"UNAVAILABLE"));
			}
		});
		
		cli.waitForRouter("router1");
		
		String command = null;
		while (true)
		{
			System.out.println("Act Router: "+cli.getActRouter()+" Act Context: "+cli.getActContext());
			String[] availableRouters = cli.getAvailableRouters();
			if (availableRouters != null)
				System.out.println("Available Routers: "+concat(availableRouters,", "));
			
			System.out.print("Command: ");
			command = readLine();
			if (command == null)
				break;
			try {
				cli.executeCommand(command.trim());
			} catch (CLIException e)
			{
				System.out.println(e.getMessage());
			}
		}
		
		try {
			cli.close();
			connection.close();
		} catch (Exception ignored){}
	}

}

