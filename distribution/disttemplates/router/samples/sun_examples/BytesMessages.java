/*
 * @(#)BytesMessages.java	1.2 00/06/02
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
import java.io.*;
import javax.jms.*;
import javax.naming.*;

/**
 * The BytesMessages class consists only of a main method, which writes and
 * reads a BytesMessage of indeterminate length.  It does not send the message.
 * <p>
 * Specify a text file name on the command line when you run the program.
 * <p>
 * This is not a realistic example of the use of the BytesMessage message type,
 * which is intended for client encoding of existing message formats.  (If 
 * possible, one of the other message types, such as StreamMessage or
 * MapMessage, should be used instead.)  However, it shows how to use a buffer
 * to write or read a BytesMessage when you do not know its length.
 *
 * @author Kim Haase
 * @version 1.2, 06/02/00
 */
public class BytesMessages {

    /**
     * Main method.
     *
     * @param args	the name of the text file used by the example
     */
    public static void main(String[] args) {
        QueueConnectionFactory       queueConnectionFactory = null;
        QueueConnection              queueConnection = null;
        QueueSession                 queueSession = null;
        int                          exitResult = 0;

        BytesMessage                 bytesMessage = null;
        
        FileInputStream              inStream = null;
        String                       filename = null;
        int                          bytes_read = 0;
        final int                    BUFLEN = 64;
        byte[]                       buf1 = new byte[BUFLEN];
        byte[]                       buf2 = new byte[BUFLEN];
        int                          length = 0;

    	/*
    	 * Read text file name from command line and create input stream.
    	 */
    	if (args.length != 1) {
    	    System.out.println("Usage: java BytesMessages <filename>");
    	    System.exit(1);
    	}
    	try {
    	    filename = new String(args[0]);
            inStream = new FileInputStream(filename);
    	} catch (IOException e) {
    	    System.out.println("Problem getting file: " + e.toString());
            System.exit(1);
    	}
    	
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
             * Create a BytesMessage.
             * Read a byte stream from the input stream into a buffer and
             * construct a BytesMessage, using the three-argument form 
             * of the writeBytes method to ensure that the message contains 
             * only the bytes read from the file, not any leftover characters 
             * in the buffer.
             */
            bytesMessage = queueSession.createBytesMessage();
            while ((bytes_read = inStream.read(buf1)) != -1) {
                bytesMessage.writeBytes(buf1, 0, bytes_read);
                System.out.println("Writing " + bytes_read 
                    + " bytes into message");
            }
            
            /*
             * Reset the message to the beginning, then use readBytes to
             * extract its contents into another buffer, casting the byte array
             * elements to char so that they will display intelligibly.
             */
            bytesMessage.reset();
            do {
                length = bytesMessage.readBytes(buf2);
                if (length != -1) {
                    System.out.println("Reading " + length
                        + " bytes from message: ");
                    for (int i = 0; i < length; i++) {
                        System.out.print((char)buf2[i]);
                    }
                }
                System.out.println();
            } while (length >= BUFLEN);
        } catch (JMSException e) {
            System.out.println("JMS exception occurred: " + e.toString());
            exitResult = 1;
        } catch (IOException e) {
            System.out.println("I/O exception occurred: " + e.toString());
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
