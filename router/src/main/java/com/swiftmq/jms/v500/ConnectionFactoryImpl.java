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

package com.swiftmq.jms.v500;

import com.swiftmq.client.thread.PoolManager;
import com.swiftmq.jms.CompoundConnectionFactory;
import com.swiftmq.net.SocketFactory;
import com.swiftmq.net.client.BlockingConnection;
import com.swiftmq.net.client.IntraVMConnection;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.net.NetworkSwiftlet;
import com.swiftmq.tools.dump.Dumpable;

import javax.jms.*;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;

public class ConnectionFactoryImpl
        implements CompoundConnectionFactory, Referenceable, Serializable, Dumpable {
    String listenerName = null;
    String socketFactoryClass = null;
    SocketFactory socketFactory = null;
    String hostname = null;
    int port = 0;
    long keepaliveInterval = 0;
    String clientId = null;
    int smqpProducerReplyInterval = 0;
    int smqpConsumerCacheSize = 0;
    int jmsDeliveryMode = 0;
    int jmsPriority = 0;
    long jmsTTL = 0;
    boolean jmsMessageIdEnabled = false;
    boolean jmsMessageTimestampEnabled = false;
    boolean useThreadContextCL = false;
    int inputBufferSize = 0;
    int inputExtendSize = 0;
    int outputBufferSize = 0;
    int outputExtendSize = 0;
    boolean intraVM = false;

    public ConnectionFactoryImpl(String listenerName, String socketFactoryClass, String hostname, int port, long keepaliveInterval,
                                 String clientId, int smqpProducerReplyInterval, int smqpConsumerCacheSize, int jmsDeliveryMode,
                                 int jmsPriority, long jmsTTL, boolean jmsMessageIdEnabled, boolean jmsMessageTimestampEnabled,
                                 boolean useThreadContextCL, int inputBufferSize, int inputExtendSize,
                                 int outputBufferSize, int outputExtendSize, boolean intraVM) {
        this.listenerName = listenerName;
        this.socketFactoryClass = socketFactoryClass;
        this.hostname = hostname;
        this.port = port;
        this.keepaliveInterval = keepaliveInterval;
        this.clientId = clientId;
        this.smqpProducerReplyInterval = smqpProducerReplyInterval;
        this.smqpConsumerCacheSize = smqpConsumerCacheSize;
        this.jmsDeliveryMode = jmsDeliveryMode;
        this.jmsPriority = jmsPriority;
        this.jmsTTL = jmsTTL;
        this.jmsMessageIdEnabled = jmsMessageIdEnabled;
        this.jmsMessageTimestampEnabled = jmsMessageTimestampEnabled;
        this.useThreadContextCL = useThreadContextCL;
        this.inputBufferSize = inputBufferSize;
        this.inputExtendSize = inputExtendSize;
        this.outputBufferSize = outputBufferSize;
        this.outputExtendSize = outputExtendSize;
        this.intraVM = intraVM;
    }

    public ConnectionFactoryImpl(String socketFactoryClass, String hostname, int port, long keepaliveInterval) {
        this(null, socketFactoryClass, hostname, port, keepaliveInterval, null, 20, 500,
                Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, true, true, false,
                131072, 65536, 131072, 65536, false);
    }

    public ConnectionFactoryImpl() {
    }

    public int getDumpId() {
        return 0;
    }

    private void writeDump(DataOutput out, String s) throws IOException {
        if (s != null) {
            out.writeByte(1);
            out.writeUTF(s);
        } else
            out.writeByte(0);
    }

    public void writeContent(DataOutput out)
            throws IOException {
        writeDump(out, listenerName);
        writeDump(out, socketFactoryClass);
        writeDump(out, hostname);
        out.writeInt(port);
        out.writeLong(keepaliveInterval);
        writeDump(out, clientId);
        out.writeInt(smqpProducerReplyInterval);
        out.writeInt(smqpConsumerCacheSize);
        out.writeInt(jmsDeliveryMode);
        out.writeInt(jmsPriority);
        out.writeLong(jmsTTL);
        out.writeBoolean(jmsMessageIdEnabled);
        out.writeBoolean(jmsMessageTimestampEnabled);
        out.writeBoolean(useThreadContextCL);
        out.writeInt(inputBufferSize);
        out.writeInt(inputExtendSize);
        out.writeInt(outputBufferSize);
        out.writeInt(outputExtendSize);
        out.writeBoolean(intraVM);
    }

    private String readDump(DataInput in) throws IOException {
        String s = null;
        if (in.readByte() == 1)
            s = in.readUTF();
        return s;
    }

    public void readContent(DataInput in)
            throws IOException {
        listenerName = readDump(in);
        socketFactoryClass = readDump(in);
        hostname = readDump(in);
        port = in.readInt();
        keepaliveInterval = in.readLong();
        clientId = readDump(in);
        smqpProducerReplyInterval = in.readInt();
        smqpConsumerCacheSize = in.readInt();
        jmsDeliveryMode = in.readInt();
        jmsPriority = in.readInt();
        jmsTTL = in.readLong();
        jmsMessageIdEnabled = in.readBoolean();
        jmsMessageTimestampEnabled = in.readBoolean();
        useThreadContextCL = in.readBoolean();
        inputBufferSize = in.readInt();
        inputExtendSize = in.readInt();
        outputBufferSize = in.readInt();
        outputExtendSize = in.readInt();
        intraVM = in.readBoolean();
    }

    public Reference getReference() throws NamingException {
        Reference ref = new Reference(ConnectionFactoryImpl.class.getName(),
                new StringRefAddr("listenerName", listenerName),
                com.swiftmq.jndi.v400.SwiftMQObjectFactory.class.getName(),
                null);
        ref.add(new StringRefAddr("socketFactoryClass", socketFactoryClass));
        ref.add(new StringRefAddr("hostname", hostname));
        ref.add(new StringRefAddr("port", String.valueOf(port)));
        ref.add(new StringRefAddr("keepaliveInterval", String.valueOf(keepaliveInterval)));
        if (clientId != null)
            ref.add(new StringRefAddr("clientId", clientId));
        ref.add(new StringRefAddr("smqpProducerReplyInterval", String.valueOf(smqpProducerReplyInterval)));
        ref.add(new StringRefAddr("smqpConsumerCacheSize", String.valueOf(smqpConsumerCacheSize)));
        ref.add(new StringRefAddr("jmsDeliveryMode", String.valueOf(jmsDeliveryMode)));
        ref.add(new StringRefAddr("jmsPriority", String.valueOf(jmsPriority)));
        ref.add(new StringRefAddr("jmsTTL", String.valueOf(jmsTTL)));
        ref.add(new StringRefAddr("jmsMessageIdEnabled", String.valueOf(jmsMessageIdEnabled)));
        ref.add(new StringRefAddr("jmsMessageTimestampEnabled", String.valueOf(jmsMessageTimestampEnabled)));
        ref.add(new StringRefAddr("useThreadContextCL", String.valueOf(useThreadContextCL)));
        ref.add(new StringRefAddr("inputBufferSize", String.valueOf(inputBufferSize)));
        ref.add(new StringRefAddr("inputExtendSize", String.valueOf(inputExtendSize)));
        ref.add(new StringRefAddr("outputBufferSize", String.valueOf(outputBufferSize)));
        ref.add(new StringRefAddr("outputExtendSize", String.valueOf(outputExtendSize)));
        ref.add(new StringRefAddr("intraVM", String.valueOf(intraVM)));
        return ref;
    }

    public String getListenerName() {
        return listenerName;
    }

    private com.swiftmq.net.client.Connection createServerConnection() throws JMSException {
        PoolManager.setIntraVM(intraVM);
        com.swiftmq.net.client.Connection conn = null;
        if (intraVM) {
            try {
                conn = new IntraVMConnection();
                NetworkSwiftlet networkSwiftlet = (NetworkSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$net");
                networkSwiftlet.connectIntraVMListener("sys$jms", (IntraVMConnection) conn);
            } catch (Exception e) {
                throw new JMSException("error creating intraVM connection, message: " + e.getMessage());
            }
        } else {
            try {
                if (socketFactory == null)
                    socketFactory = (SocketFactory) Class.forName(socketFactoryClass).newInstance();
                Socket socket = socketFactory.createSocket(hostname, port);
                conn = new BlockingConnection(socket, inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize);
            } catch (Exception e) {
                throw new JMSException("error creating socket connection to "
                        + hostname + ":" + port + ", message: "
                        + e.getMessage());
            }
        }
        return conn;
    }

    // --> JMS 1.1
    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    public synchronized Connection createConnection(String userName, String password) throws JMSException {
        // create server connection
        com.swiftmq.net.client.Connection conn = createServerConnection();

        // create connection and return it
        ConnectionImpl qc = new ConnectionImpl(userName == null ? "anonymous" : userName, password, conn);
        qc.assignClientId(clientId);
        qc.setSmqpProducerReplyInterval(smqpProducerReplyInterval);
        qc.setSmqpConsumerCacheSize(smqpConsumerCacheSize);
        qc.setJmsDeliveryMode(jmsDeliveryMode);
        qc.setJmsPriority(jmsPriority);
        qc.setJmsTTL(jmsTTL);
        qc.setJmsMessageIdEnabled(jmsMessageIdEnabled);
        qc.setJmsMessageTimestampEnabled(jmsMessageTimestampEnabled);
        qc.setUseThreadContextCL(useThreadContextCL);
        if (keepaliveInterval > 0)
            qc.startKeepAlive(keepaliveInterval);
        return (qc);
    }

    public XAConnection createXAConnection() throws JMSException {
        return createXAConnection(null, null);
    }

    public synchronized XAConnection createXAConnection(String userName, String password) throws JMSException {
        // create server connection
        com.swiftmq.net.client.Connection conn = createServerConnection();

        // create connection and return it
        XAConnectionImpl qc = new XAConnectionImpl(userName == null ? "anonymous" : userName, password, conn);
        qc.assignClientId(clientId);
        qc.setSmqpProducerReplyInterval(smqpProducerReplyInterval);
        qc.setSmqpConsumerCacheSize(smqpConsumerCacheSize);
        qc.setJmsDeliveryMode(jmsDeliveryMode);
        qc.setJmsPriority(jmsPriority);
        qc.setJmsTTL(jmsTTL);
        qc.setJmsMessageIdEnabled(jmsMessageIdEnabled);
        qc.setJmsMessageTimestampEnabled(jmsMessageTimestampEnabled);
        qc.setUseThreadContextCL(useThreadContextCL);
        if (keepaliveInterval > 0)
            qc.startKeepAlive(keepaliveInterval);
        return (qc);
    }
    // <-- JMS 1.1

    public QueueConnection createQueueConnection() throws JMSException {
        return (createQueueConnection(null, null));
    }

    public synchronized QueueConnection createQueueConnection(String userName,
                                                              String password) throws JMSException {
        // create server connection
        com.swiftmq.net.client.Connection conn = createServerConnection();

        // create queue connection and return it
        QueueConnectionImpl qc = new QueueConnectionImpl(userName == null ? "anonymous" : userName, password, conn);
        qc.assignClientId(clientId);
        qc.setSmqpProducerReplyInterval(smqpProducerReplyInterval);
        qc.setSmqpConsumerCacheSize(smqpConsumerCacheSize);
        qc.setJmsDeliveryMode(jmsDeliveryMode);
        qc.setJmsPriority(jmsPriority);
        qc.setJmsTTL(jmsTTL);
        qc.setJmsMessageIdEnabled(jmsMessageIdEnabled);
        qc.setJmsMessageTimestampEnabled(jmsMessageTimestampEnabled);
        qc.setUseThreadContextCL(useThreadContextCL);
        if (keepaliveInterval > 0)
            qc.startKeepAlive(keepaliveInterval);
        return (qc);
    }

    public XAQueueConnection createXAQueueConnection() throws JMSException {
        return (createXAQueueConnection(null, null));
    }

    public XAQueueConnection createXAQueueConnection(String userName, String password) throws JMSException {
        // create server connection
        com.swiftmq.net.client.Connection conn = createServerConnection();

        // create queue connection and return it
        XAQueueConnectionImpl qc = new XAQueueConnectionImpl(userName == null ? "anonymous" : userName, password, conn);
        qc.assignClientId(clientId);
        qc.setSmqpProducerReplyInterval(smqpProducerReplyInterval);
        qc.setSmqpConsumerCacheSize(smqpConsumerCacheSize);
        qc.setJmsDeliveryMode(jmsDeliveryMode);
        qc.setJmsPriority(jmsPriority);
        qc.setJmsTTL(jmsTTL);
        qc.setJmsMessageIdEnabled(jmsMessageIdEnabled);
        qc.setJmsMessageTimestampEnabled(jmsMessageTimestampEnabled);
        qc.setUseThreadContextCL(useThreadContextCL);
        if (keepaliveInterval > 0)
            qc.startKeepAlive(keepaliveInterval);
        return (qc);
    }

    public TopicConnection createTopicConnection()
            throws JMSException {
        return (createTopicConnection(null, null));
    }

    public synchronized TopicConnection createTopicConnection(String userName, String password)
            throws JMSException {
        // create server connection
        com.swiftmq.net.client.Connection conn = createServerConnection();

        // create queue connection and return it
        TopicConnectionImpl qc = new TopicConnectionImpl(userName == null ? "anonymous" : userName, password, conn);
        qc.assignClientId(clientId);
        qc.setSmqpProducerReplyInterval(smqpProducerReplyInterval);
        qc.setSmqpConsumerCacheSize(smqpConsumerCacheSize);
        qc.setJmsDeliveryMode(jmsDeliveryMode);
        qc.setJmsPriority(jmsPriority);
        qc.setJmsTTL(jmsTTL);
        qc.setJmsMessageIdEnabled(jmsMessageIdEnabled);
        qc.setJmsMessageTimestampEnabled(jmsMessageTimestampEnabled);
        qc.setUseThreadContextCL(useThreadContextCL);
        if (keepaliveInterval > 0)
            qc.startKeepAlive(keepaliveInterval);

        return (qc);
    }

    public XATopicConnection createXATopicConnection() throws JMSException {
        return (createXATopicConnection(null, null));
    }

    public XATopicConnection createXATopicConnection(String userName, String password) throws JMSException {
        // create server connection
        com.swiftmq.net.client.Connection conn = createServerConnection();

        // create queue connection and return it
        XATopicConnectionImpl qc = new XATopicConnectionImpl(userName == null ? "anonymous" : userName, password, conn);
        qc.assignClientId(clientId);
        qc.setSmqpProducerReplyInterval(smqpProducerReplyInterval);
        qc.setSmqpConsumerCacheSize(smqpConsumerCacheSize);
        qc.setJmsDeliveryMode(jmsDeliveryMode);
        qc.setJmsPriority(jmsPriority);
        qc.setJmsTTL(jmsTTL);
        qc.setJmsMessageIdEnabled(jmsMessageIdEnabled);
        qc.setJmsMessageTimestampEnabled(jmsMessageTimestampEnabled);
        qc.setUseThreadContextCL(useThreadContextCL);
        if (keepaliveInterval > 0)
            qc.startKeepAlive(keepaliveInterval);

        return (qc);
    }

    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("[ConnectionFactoryImpl, socketFactoryClass=");
        s.append(socketFactoryClass);
        s.append(", hostname=");
        s.append(hostname);
        s.append(", port=");
        s.append(port);
        s.append(", keepaliveInterval=");
        s.append(keepaliveInterval);
        s.append(", clientId=");
        s.append(clientId);
        s.append(", smqpProducerReplyInterval=");
        s.append(smqpProducerReplyInterval);
        s.append(", smqpConsumerCacheSize=");
        s.append(smqpConsumerCacheSize);
        s.append(", jmsDeliveryMode=");
        s.append(jmsDeliveryMode);
        s.append(", jmsPriority=");
        s.append(jmsPriority);
        s.append(", jmsTTL=");
        s.append(jmsTTL);
        s.append(", jmsMessageIdEnabled=");
        s.append(jmsMessageIdEnabled);
        s.append(", jmsMessageTimestampEnabled=");
        s.append(jmsMessageTimestampEnabled);
        s.append(", useThreadContextCL=");
        s.append(useThreadContextCL);
        s.append(", inputBufferSize=");
        s.append(inputBufferSize);
        s.append(", inputExtendSize=");
        s.append(inputExtendSize);
        s.append(", outputBufferSize=");
        s.append(outputBufferSize);
        s.append(", outputExtendSize=");
        s.append(outputExtendSize);
        s.append(", intraVM=");
        s.append(intraVM);
        s.append("]");
        return s.toString();
    }
}



