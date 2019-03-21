/*
 * @(#)SampleUtilities.java	1.2 00/06/02
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
import javax.naming.*;
import java.io.*;
import java.util.*;

/**
 * Utility class for JMS sample programs.
 * <p>
 * Contains the following methods:  jndiLookup, exit, wait_for_quit
 *
 * @author Kim Haase
 * @version 1.2, 06/02/00
 */
public class SampleUtilities {
	public static final String QUEUECONFAC = "QueueConnectionFactory";
	public static final String TOPICCONFAC = "TopicConnectionFactory";
	private static Context   jndiContext = null;
	private static Object    obj = null;
	
	/**
	 * Creates a JNDI InitialContext object if none exists yet.  Then looks up 
	 * the string argument and returns the associated object.
	 *
	 * @param name	the name of the object to be looked up
	 *
	 * @return		the object bound to <code>name</code>
	 * @throws		javax.naming.NamingException
	 */
	public static Object jndiLookup(String name) throws NamingException {
		if (jndiContext == null) {
			try {
				// Begin SwiftMQ
				Hashtable env = new Hashtable();
				env.put(Context.INITIAL_CONTEXT_FACTORY,"com.swiftmq.jndi.InitialContextFactoryImpl");
				env.put(Context.PROVIDER_URL,"smqp://localhost:4001/timeout=10000");
				jndiContext = new InitialContext(env);
				// End SwiftMQ
				//jndiContext = new InitialContext();
			} catch (NamingException e) {
				System.out.println("Could not create JNDI context: " + 
					e.toString());
				throw e;
			}
		}
		try {
			obj = jndiContext.lookup(name);
		} catch (NamingException e) {
			System.out.println("JNDI lookup failed: " + e.toString());
			// Begin SwiftMQ
			// A note concerning the most famous handling mistake...
			System.out.println();
			System.out.println("==== NOTE ====");
			System.out.println("If your JNDI lookup refers to a queue,");
			System.out.println("please make sure that you've specified");
			System.out.println("the queue name fully qualified with the");
			System.out.println("router name as <queue>@<router>.");
			System.out.println("For example: testqueue@router1");
			System.out.println("==== NOTE ====");
			System.out.println();
			// End SwiftMQ
			throw e;
		}
		
		return obj;
	}
	
	/**
	 * The exit method is needed because of an RMI/IIOP bug that causes JMS 
	 * programs to exit only with an explicit call to System.exit().
	 * 
	 * @param result	The exit result; 0 indicates no errors
	 */
	public static void exit(int result) {
		
		System.exit(result);
	}
	
	/**
	 * Waits for the user to enter Q or q (followed by a return).
	 *
	 * @return	the exit status (nonzero indicates abnormal termination)
	 */
	public static int wait_for_quit() {
		InputStreamReader inputStreamReader = new InputStreamReader(System.in);
		char              answer = '\0';
		int               exitResult = 0;
		
		while (!((answer == 'q') || (answer == 'Q'))) {
			try {
				answer = (char) inputStreamReader.read();
			} catch (IOException e) {
				System.out.println("I/O exception: " + e.toString());
				exitResult = 1;
			}
		}
		return exitResult;
	}
}
