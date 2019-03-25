/*
 * Copyright 2019 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.swiftmq.filetransfer;

import com.swiftmq.filetransfer.protocol.*;
import com.swiftmq.filetransfer.v940.LinkParser;
import com.swiftmq.jms.SwiftMQConnection;
import com.swiftmq.tools.requestreply.RequestRegistry;

import javax.jms.*;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Reliable Filetransfer over JMS.
 * </p>
 * <p>
 * This class contains 2 factory methods to create a new Filetransfer object. One to send files, the other to receive files.
 * A JMS connection is passed as a parameter. A new dedicated JMS session is created from this JMS connection to perform the transfer so
 * files can be transferred by different Filetransfer objects in different threads. See the Filetransfer samples
 * which are contained in the SwiftMQ documentation.
 * </p>
 * <p>
 * Have a look at the <a href="http://www.swiftmq.com/products/router/swiftlets/sys_filecache/client/index.html">User's Guide</a>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2013, All Rights Reserved
 */
public abstract class Filetransfer {
    private static final int PROTOCOL_VERSION = Integer.parseInt(System.getProperty("swiftmq.filetransfer.protocol.version", "941"));

    protected Filetransfer() {
    }

    /**
     * Creates a new Filetransfer which is connected to a filecache which is identified by router name and cache name.
     * <p/>
     * The returned Filetransfer object holds a dedicated JMS session which is created from the connection passed as parameter.
     *
     * @param connection JMS connection
     * @param routerName router name
     * @param cacheName  cache name
     * @return connected Filetransfer object
     * @throws FiletransferException if there is an error during the file transfer
     * @throws JMSException          if there is an error during JMS access
     */
    public static Filetransfer create(Connection connection, String routerName, String cacheName) throws FiletransferException, JMSException {
        return createAndSetup(connection, routerName, cacheName);
    }

    /**
     * Creates a new Filetransfer which is connected to a filecache which is identified by a link.
     * <p/>
     * The returned Filetransfer object holds a dedicated JMS session which is created from the connection passed as parameter.
     *
     * @param connection JMS connection
     * @param link       link to a file
     * @return connected Filetransfer object
     * @throws FiletransferException if there is an error during the file transfer
     * @throws JMSException          if there is an error during JMS access
     */
    public static Filetransfer create(Connection connection, String link) throws FiletransferException, JMSException {
        LinkParser linkParser = new LinkParser(link);
        return createAndSetup(connection, linkParser.getRouterName(), linkParser.getCacheName()).withLink(link);
    }

    private static Filetransfer createAndSetup(Connection connection, String routerName, String cacheName) throws FiletransferException, JMSException {
        JMSAccessorHolder accessorHolder = new JMSAccessorHolder();
        accessorHolder.username = ((SwiftMQConnection) connection).getUserName();
        accessorHolder.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        accessorHolder.replyQueue = accessorHolder.session.createTemporaryQueue();
        accessorHolder.producer = accessorHolder.session.createProducer(accessorHolder.session.createQueue(Util.createCacheRequestQueueName(cacheName, routerName)));
        accessorHolder.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        accessorHolder.consumer = accessorHolder.session.createConsumer(accessorHolder.replyQueue);
        ProtocolReply reply = (ProtocolReply) request(new ProtocolRequest(PROTOCOL_VERSION), accessorHolder, new com.swiftmq.filetransfer.protocol.ProtocolFactory());
        Filetransfer filetransfer = null;
        if (reply.isOk()) {

            // Close old producer on the swiftmqfiletransfer queue
            accessorHolder.producer.close();

            // Open a new one on the temp queue for this particular transfer
            accessorHolder.producer = accessorHolder.session.createProducer(accessorHolder.session.createQueue(reply.getQueueName()));

            // Create versioned Filetransfer
            switch (PROTOCOL_VERSION) {
                case 940:
                    filetransfer = new com.swiftmq.filetransfer.v940.FiletransferImpl(accessorHolder);
                    break;
                case 941:
                    filetransfer = new com.swiftmq.filetransfer.v941.FiletransferImpl(accessorHolder);
                    break;
            }
        } else
            throw new FiletransferException(reply.getException());
        return filetransfer;
    }

    protected static MessageBasedReply request(MessageBasedRequest request, JMSAccessorHolder accessorHolder, MessageBasedFactory factory) throws FiletransferException, JMSException {
        return request(request, accessorHolder, factory, null);
    }

    protected static MessageBasedReply request(MessageBasedRequest request, JMSAccessorHolder accessorHolder, MessageBasedFactory factory, Map<String, Object> properties) throws FiletransferException, JMSException {
        Message msg = request.toMessage();
        msg.setJMSReplyTo(accessorHolder.replyQueue);
        if (properties != null) {
            for (Iterator<Map.Entry<String, Object>> iter = properties.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<String, Object> entry = iter.next();
                msg.setObjectProperty(entry.getKey(), entry.getValue());
            }
        }
        accessorHolder.producer.send(msg);
        if (!request.isReplyRequired())
            return null;
        msg = accessorHolder.consumer.receive(RequestRegistry.SWIFTMQ_REQUEST_TIMEOUT);
        if (msg == null)
            throw new JMSException("Request timeout occured (" + RequestRegistry.SWIFTMQ_REQUEST_TIMEOUT + ") ms");
        return (MessageBasedReply) factory.create(msg);
    }

    /**
     * Sets the file to send or receive.
     *
     * @param file the file
     * @return this Filetransfer object
     */
    public abstract Filetransfer withFile(File file);

    /**
     * Overwrites the name of the file to send. So one can send a file
     * under a different name.
     *
     * @param filename name the file
     * @return this Filetransfer object
     */
    public abstract Filetransfer withFilename(String filename);

    /**
     * Sets the output directory where received files are stored.
     * This has only effect by using withOriginalFilename(true)
     *
     * @param outputDir Output directory
     * @return this Filetransfer object
     */
    public abstract Filetransfer withOutputDirectory(File outputDir);

    /**
     * Enables/disables the use of the original filenames of the receiving files. Default is true.
     * Requires to set the output directory.
     *
     * @param withOriginalFilename true/false
     * @return this Filetransfer object
     */
    public abstract Filetransfer withOriginalFilename(boolean withOriginalFilename);

    /**
     * Sets the link of a file to receive.
     *
     * @param link link to a file
     * @return this Filetransfer object
     */
    public abstract Filetransfer withLink(String link);

    /**
     * Sets the type of the digest that is used to verify the consistency of the file transfer.
     * <p/>
     * The digest type must be one of the availables types of the platform. The sender defines
     * this type for a file and the receiver of the file must use the same type. Default is "MD5".
     *
     * @param digestType type of the message digest
     * @return this Filetransfer object
     */
    public abstract Filetransfer withDigestType(String digestType);

    /**
     * The sender can define a maximum number of downloads for a file. The file will be automatically
     * deleted when this number is reached. Default is unlimited downloads so the files needs to
     * be deleted explicitely.
     *
     * @param deleteAfterNumberDownloads max number of downloads
     * @return this Filetransfer object
     */
    public abstract Filetransfer withDeleteAfterNumberDownloads(int deleteAfterNumberDownloads);

    /**
     * Sets the password for a file. During send this method defines the password, during receive
     * this method specifies it. Passwords are not being transferred as clear text, instead a hash is used.
     *
     * @param password the password
     * @return this Filetransfer object
     */
    public abstract Filetransfer withPassword(String password);

    /**
     * For internal use only!
     *
     * @param passwordHexDigest the password hex digest
     * @return this Filetransfer object
     */
    public abstract Filetransfer withPasswordHexDigest(String passwordHexDigest);

    /**
     * The sender can define an expiration time in millisecond relative to the time when the file is
     * store at the file cache. So if the file is stored at 13:45:10 and the expiration is set to
     * 60000 (1 minute), the file expires at 13:46:10. Expired files will be automatically deleted
     * from the file cache. Default is no expiration.
     *
     * @param expiration expiration time in milliseconds.
     * @return this Filetransfer object
     */
    public abstract Filetransfer withExpiration(long expiration);

    /**
     * A sender can declare a file as private. Private files are not returned by a query, rather the link
     * to that file must be passed from the sender to the receiver (e.g. by using JMS). Default is false.
     *
     * @param fileIsPrivate true/false
     * @return this Filetransfer object
     */
    public abstract Filetransfer withFileIsPrivate(boolean fileIsPrivate);

    /**
     * A sender can add any custom properties to a file. The names and types of the properties have to adhere
     * to the JMS standard (message.setObject(name, value) must succeed for each property).
     *
     * @param properties Map with properties
     * @return this Filetransfer object
     */
    public abstract Filetransfer withProperties(Map<String, Object> properties);

    /**
     * Sets a JMS message selector to use by a query.
     *
     * @param selector JMS message selector
     * @return this Filetransfer object
     */
    public abstract Filetransfer withSelector(String selector);

    /**
     * Sets the internal message transfer interval in which a consistency check is performed. Default is 10.
     *
     * @param replyInterval reply interval
     * @return this Filetransfer object
     */
    public abstract Filetransfer withReplyInterval(int replyInterval);

    /**
     * Performs the sending file transfer and notifies a progress listener during the transfer.
     *
     * @param progressListener progress listener
     * @return link to the file
     * @throws Exception if anything goes wrong
     */
    public abstract String send(ProgressListener progressListener) throws Exception;

    /**
     * Performs the sending file transfer.
     *
     * @return link to the file
     * @throws Exception if anything goes wrong
     */
    public abstract String send() throws Exception;

    /**
     * Performs the receiving file transfer and notifies a progress listener during the transfer.
     *
     * @param progressListener progress listener
     * @return this Filetransfer object
     * @throws Exception if anything goes wrong
     */
    public abstract Filetransfer receive(ProgressListener progressListener) throws Exception;

    /**
     * Performs the receiving file transfer.
     *
     * @return this Filetransfer object
     * @throws Exception if anything goes wrong
     */
    public abstract Filetransfer receive() throws Exception;

    /**
     * Deletes a file. Requires that the link has been set. Usually called after receive()
     *
     * @return this Filetransfer object
     * @throws Exception if anything goes wrong
     */
    public abstract Filetransfer delete() throws Exception;

    /**
     * Queries the file cache and returns a list of links. If the selector is set, it is applied, otherwise
     * the links to all files of the cache are returned. Links to private files are never returned.
     *
     * @return result
     * @throws Exception if anything goes wrong
     */
    public abstract List<String> query() throws Exception;

    /**
     * Queries the file cache and returns a map where the key is the link and the value is another Map with all
     * properties of that file. If the selector is set, it is applied. If a link is set, only the properties of that
     * link are returned, otherwise the links/properties of all files of the cache are returned.
     * Properties of private files are never returned.
     *
     * @return result
     * @throws Exception if anything goes wrong
     */
    public abstract Map<String, Map<String, Object>> queryProperties() throws Exception;

    /**
     * Closes this Filetransfer object and releases all resources (e.g. the internal JMS session)
     */
    public abstract void close();

}
