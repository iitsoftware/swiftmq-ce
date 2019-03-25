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

package com.swiftmq.jndi.v400;

import com.swiftmq.client.Versions;
import com.swiftmq.jms.*;
import com.swiftmq.jndi.StopRetryException;
import com.swiftmq.jndi.protocol.v400.*;
import com.swiftmq.swiftlet.jndi.JNDISwiftlet;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.versioning.Versionable;
import com.swiftmq.tools.versioning.Versioned;

import javax.jms.*;
import javax.naming.*;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class ContextImpl implements Context, java.io.Serializable {
    Hashtable env = null;
    JNDIInfo jndiInfo = null;
    Connection connection = null;
    Session session = null;
    MessageProducer producer = null;
    boolean closed = true;

    public ContextImpl(Hashtable env)
            throws NamingException {
        this.env = env;
        String url = (String) env.get(Context.PROVIDER_URL);
        if (url == null)
            throw new NamingException("missing JNDI environment property: Context.PROVIDER_URL (" + Context.PROVIDER_URL + ")");
        jndiInfo = URLParser.parseURL(url);
        Map props = new HashMap();
        if (jndiInfo.isIntraVM()) {
            try {
                props.put(SwiftMQConnectionFactory.INTRAVM, "true");
            } catch (Exception e) {
                throw new NamingException("unable to connect, exception = " + e);
            }
        } else {
            String factoryClass = jndiInfo.getFactory();
            try {
                Class.forName(factoryClass);
            } catch (Exception e) {
                throw new NamingException("socket factory not found: " + factoryClass);
            }
            try {
                props.put(SwiftMQConnectionFactory.SOCKETFACTORY, factoryClass);
                props.put(SwiftMQConnectionFactory.HOSTNAME, jndiInfo.getHostname());
                props.put(SwiftMQConnectionFactory.PORT, String.valueOf(jndiInfo.getPort()));
                props.put(SwiftMQConnectionFactory.KEEPALIVEINTERVAL, String.valueOf(jndiInfo.getKeepalive()));
            } catch (Exception e) {
                throw new NamingException("unable to connect, exception = " + e);
            }
        }
        try {
            ConnectionFactory cf = (ConnectionFactory) SwiftMQConnectionFactory.create(props);
            connection = cf.createConnection(jndiInfo.getUsername(), jndiInfo.getPassword());
            session = connection.createSession(false, 0);
            producer = session.createProducer(null);
            connection.start();
            closed = false;
        } catch (Exception e) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e1) {
                }
            }
            closed = true;
            if ((e instanceof JMSSecurityException) || (e instanceof InvalidVersionException))
                throw new StopRetryException(e.getMessage());
            throw new NamingException("unable to connect, exception = " + e);
        }
    }

    public Object addToEnvironment(String name, Object value)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    private Versioned createVersioned(int version, JNDIRequest request) throws Exception {
        DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
        Dumpalizer.dump(dos, request);
        return new Versioned(version, dos.getBuffer(), dos.getCount());
    }

    private BytesMessageImpl createMessage(Versionable versionable, Destination replyTo) throws Exception {
        BytesMessageImpl msg = new BytesMessageImpl();
        versionable.transferToMessage(msg);
        if (replyTo != null)
            msg.setJMSReplyTo(replyTo);
        return msg;
    }

    public synchronized void bind(String name, Object obj)
            throws NamingException {
        if (closed)
            throw new NamingException("context is closed!");
        if (!(obj instanceof TemporaryTopicImpl || obj instanceof TemporaryQueueImpl))
            throw new OperationNotSupportedException("bind is only supported for TemporaryQueues/TemporaryTopics!");
        try {
            TemporaryTopic tt = session.createTemporaryTopic();
            MessageConsumer consumer = session.createConsumer(tt);

            Versionable versionable = new Versionable();
            versionable.addVersioned(Versions.JNDI_CURRENT, createVersioned(Versions.JNDI_CURRENT, new BindRequest(name, (QueueImpl) obj)),
                    "com.swiftmq.jndi.protocol.v" + Versions.JNDI_CURRENT + ".JNDIRequestFactory");
            BytesMessage request = createMessage(versionable, tt);
            producer.send(session.createTopic(JNDISwiftlet.JNDI_TOPIC), request, DeliveryMode.NON_PERSISTENT, MessageImpl.MAX_PRIORITY, 0);
            TextMessage reply = (TextMessage) consumer.receive();
            String text = reply.getText();
            consumer.close();
            tt.delete();
            if (text != null)
                throw new Exception(text);
        } catch (Exception e) {
            throw new NamingException("exception occurred during bind: " + e);
        }
    }

    public void bind(Name p0, Object p1)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public synchronized void close()
            throws NamingException {
        try {
            connection.close();
        } catch (Exception ignored) {
        }
        closed = true;
    }

    public String composeName(String p0, String p1)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public Name composeName(Name p0, Name p1)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public Context createSubcontext(String p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public Context createSubcontext(Name p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public void destroySubcontext(String p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public void destroySubcontext(Name p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public Hashtable getEnvironment()
            throws NamingException {
        return env;
    }

    public String getNameInNamespace()
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public NameParser getNameParser(String p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public NameParser getNameParser(Name p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public NamingEnumeration list(String p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public NamingEnumeration list(Name p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public NamingEnumeration listBindings(String p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public NamingEnumeration listBindings(Name p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    private Destination getLookupDestination(Session session) throws JMSException {
        if (Versions.JMS_CURRENT < 630)
            return session.createTopic(JNDISwiftlet.JNDI_TOPIC);
        return session.createQueue(JNDISwiftlet.JNDI_QUEUE);
    }

    public synchronized Object lookup(String name)
            throws NamingException {
        if (closed)
            throw new NamingException("context is closed!");
        if (name == null)
            throw new OperationNotSupportedException("context cloning is not supported!");
        boolean connectionClosed = false;
        Object obj = null;
        try {
            TemporaryQueue tq = session.createTemporaryQueue();
            MessageConsumer consumer = session.createConsumer(tq);

            Versionable versionable = new Versionable();
            versionable.addVersioned(Versions.JNDI_CURRENT,
                    createVersioned(Versions.JNDI_CURRENT, new LookupRequest(name)),
                    "com.swiftmq.jndi.protocol.v" + Versions.JNDI_CURRENT + ".JNDIRequestFactory");
            BytesMessage request = createMessage(versionable, tq);
            producer.send(getLookupDestination(session), request, DeliveryMode.NON_PERSISTENT, MessageImpl.MAX_PRIORITY, 0);
            MessageImpl reply = null;
            if (jndiInfo.getTimeout() == 0)
                reply = (MessageImpl) consumer.receive();
            else
                reply = (MessageImpl) consumer.receive(jndiInfo.getTimeout());
            if (reply != null) {
                if (reply instanceof ObjectMessageImpl)
                    obj = ((ObjectMessageImpl) reply).getObject();
                else {
                    BytesMessageImpl msg = (BytesMessageImpl) reply;
                    Versionable vreply = Versionable.toVersionable(msg);
                    vreply.selectVersions(Versions.cutAfterIndex(Versions.getSelectedIndex(Versions.JMS_CURRENT, Versions.JMS), Versions.JMS));
                    obj = vreply.createVersionedObject();
                }
            }
            if (!((SwiftMQMessageConsumer) consumer).isClosed()) {
                consumer.close();
                tq.delete();
            } else
                connectionClosed = true;
        } catch (Exception e) {
            throw new CommunicationException("exception occurred during lookup: " + e);
        }
        if (connectionClosed)
            throw new CommunicationException("Connection lost!");
        if (obj == null)
            throw new NameNotFoundException("Name '" + name + "' not found (timeout occured)!");
        return obj;
    }

    public Object lookup(Name name)
            throws NamingException {
        return lookup(name.get(0));
    }

    public Object lookupLink(String p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public Object lookupLink(Name p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public synchronized void rebind(String name, Object obj)
            throws NamingException {
        if (closed)
            throw new NamingException("context is closed!");
        if (!(obj instanceof TemporaryTopicImpl || obj instanceof TemporaryQueueImpl))
            throw new OperationNotSupportedException("rebind is only supported for TemporaryQueues/TemporaryTopics!");
        try {
            TemporaryTopic tt = session.createTemporaryTopic();
            MessageConsumer consumer = session.createConsumer(tt);

            Versionable versionable = new Versionable();
            versionable.addVersioned(Versions.JNDI_CURRENT,
                    createVersioned(Versions.JNDI_CURRENT, new RebindRequest(name, (QueueImpl) obj)),
                    "com.swiftmq.jndi.protocol.v" + Versions.JNDI_CURRENT + ".JNDIRequestFactory");
            BytesMessage request = createMessage(versionable, tt);
            producer.send(session.createTopic(JNDISwiftlet.JNDI_TOPIC), request, DeliveryMode.NON_PERSISTENT, MessageImpl.MAX_PRIORITY, 0);
            TextMessage reply = (TextMessage) consumer.receive();
            String text = reply.getText();
            consumer.close();
            tt.delete();
            if (text != null)
                throw new Exception(text);
        } catch (Exception e) {
            throw new NamingException("exception occurred during rebind: " + e);
        }
    }

    public void rebind(Name p0, Object p1)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public Object removeFromEnvironment(String p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public void rename(String p0, String p1)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public void rename(Name p0, Name p1)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public synchronized void unbind(String name)
            throws NamingException {
        if (closed)
            throw new NamingException("context is closed!");
        try {

            Versionable versionable = new Versionable();
            versionable.addVersioned(400, createVersioned(400, new UnbindRequest(name)), "com.swiftmq.jndi.protocol.v400.JNDIRequestFactory");
            BytesMessage request = createMessage(versionable, null);
            producer.send(session.createTopic(JNDISwiftlet.JNDI_TOPIC), request, DeliveryMode.NON_PERSISTENT, MessageImpl.MAX_PRIORITY, 0);
        } catch (Exception e) {
            throw new NamingException("exception occurred during unbind: " + e);
        }
    }

    public void unbind(Name p0)
            throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
}

