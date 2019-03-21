/*
 * @(#)ObjectMessages.java	1.2 00/06/02
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
 * The ObjectMessages class consists only of a main method, which demonstrates
 * that mutable objects are copied, not passed by reference, when you use them 
 * to create message objects.
 * <p>
 * The example uses only an ObjectMessage and a BytesMessage, but the same is
 * true for all message formats.
 *
 * @author Kim Haase
 * @version 1.2, 06/02/00
 */
public class ObjectMessages {

    /**
     * Main method.  Takes no arguments.
     */
    public static void main(String[] args) {
        QueueConnectionFactory       queueConnectionFactory = null;
        QueueConnection              queueConnection = null;
        QueueSession                 queueSession = null;
        int                          exitResult = 0;
        
        ObjectMessage                objectMessage = null;
        BytesMessage                 bytesMessage = null;

        String                       object = "A String is an object.";
        byte[]                       byteArray = {3, 5, 7, 9, 11};
        final int                    ARRLEN = 5;
        byte[]                       inByteData = new byte[ARRLEN];
        int                          length = 0;

    	try {
    	    queueConnectionFactory = (QueueConnectionFactory) 
    	        SampleUtilities.jndiLookup(SampleUtilities.QUEUECONFAC);
    	    queueConnection = 
    	        queueConnectionFactory.createQueueConnection();
    	} catch (JMSException e) {
            System.out.println("Connection problem: " + e.toString());
    	    System.exit(1);
    	} catch (NamingException e) {
            System.out.println("JNDI lookup problem: " + e.toString());
    	    System.exit(1);
    	}

        try {
    	    queueSession = queueConnection.createQueueSession(false, 
    	        Session.AUTO_ACKNOWLEDGE);

    	    /* 
    	     * Create an ObjectMessage from a String.
    	     * Modify the original object.
    	     * Read the message, proving that the object in the message
             * has not changed.
             */
    	    objectMessage = queueSession.createObjectMessage();
    	    System.out.println("Writing ObjectMessage with string:  " + object);
    	    objectMessage.setObject(object);
    	    object = "I'm a different String now.";
    	    System.out.println("Changed string; object is now:  " + object);
            System.out.println("ObjectMessage contains:  " + 
                (String) objectMessage.getObject()); 

    	    /* 
    	     * Create a BytesMessage from an array.
    	     * Modify an element of the original array.
    	     * Reset and read the message, proving that contents of the message
             * have not changed.
    	     */
    	    bytesMessage = queueSession.createBytesMessage();
    	    System.out.print("Writing BytesMessage with array: ");
            for (int i = 0; i < ARRLEN; i++) {
                System.out.print(" " + byteArray[i]);
    	    }
    	    System.out.println();
    	    bytesMessage.writeBytes(byteArray);
    	    byteArray[1] = 13;
    	    System.out.print("Changed array element; array is now: ");
            for (int i = 0; i < ARRLEN; i++) {
                System.out.print(" " + byteArray[i]);
    	    }
    	    System.out.println();
    	    bytesMessage.reset();
            length = bytesMessage.readBytes(inByteData);
            System.out.print("BytesMessage contains: ");
            for (int i = 0; i < length; i++) {
                System.out.print(" " + inByteData[i]);
            }
    	    System.out.println();
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
