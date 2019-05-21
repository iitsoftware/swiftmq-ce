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

package com.swiftmq.impl.jmsbridge;

import javax.jms.Queue;
import javax.jms.QueueConnectionFactory;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * Implementation of an <tt>ObjectFactory</tt> that returns
 * requested JMS administered objects as a result of JNDI lookup's.
 * <BR><BR>
 * The factory requires JNDI properties for creating the <tt>InitialContext</tt> object.
 * <br><br>
 * <u>Example for Weblogic:</u>
 * <br><br><tt>
 * java.naming.factory.initial=weblogic.jndi.WLInitialContextFactory<br>
 * java.naming.provider.url=t3://localhost:7001
 * </tt><br><br>
 * <u>Example for MQSeries:</u>
 * <br><br><tt>
 * java.naming.factory.initial=com.sun.jndi.fscontext.RefFSContextFactory<br>
 * java.naming.provider.url=file://mqseries/jndi
 * </tt><br><br>
 * All properties are set "as is"
 * in a <tt>Hashtable</tt> that is passed as a parameter to the
 * <tt>InitialContext</tt> constructor.
 *
 * @author IIT GmbH, Bremen/Germany
 * @version 1.0
 * @see ObjectFactory
 **/

public class JNDIObjectFactory implements ObjectFactory {
    Properties prop = null;
    Context ctx = null;

    public void setProperties(Properties prop) {
        this.prop = prop;
    }

    private void createContext() throws ObjectFactoryException {
        try {
            if (prop != null)
                ctx = new InitialContext(prop);
            else
                ctx = new InitialContext();
        } catch (NamingException e) {
            throw new ObjectFactoryException("Error creating InitialContext: " + e.getMessage(), e);
        }
    }

    public QueueConnectionFactory getQueueConnectionFactory(String name)
            throws ObjectFactoryException {
        if (ctx == null)
            createContext();
        QueueConnectionFactory cf = null;
        try {
            cf = (QueueConnectionFactory) ctx.lookup(name);
        } catch (Exception e) {
            throw new ObjectFactoryException("Error lookup '" + name + "': " + e.getMessage(), e);
        }
        return cf;
    }

    public TopicConnectionFactory getTopicConnectionFactory(String name)
            throws ObjectFactoryException {
        if (ctx == null)
            createContext();
        TopicConnectionFactory cf = null;
        try {
            cf = (TopicConnectionFactory) ctx.lookup(name);
        } catch (Exception e) {
            throw new ObjectFactoryException("Error lookup '" + name + "': " + e.getMessage(), e);
        }
        return cf;
    }

    public Queue getQueue(String queue)
            throws ObjectFactoryException {
        if (ctx == null)
            createContext();
        Queue dest = null;
        try {
            dest = (Queue) ctx.lookup(queue);
        } catch (Exception e) {
            throw new ObjectFactoryException("Error lookup '" + queue + "': " + e.getMessage(), e);
        }
        return dest;
    }

    public Topic getTopic(String topic)
            throws ObjectFactoryException {
        if (ctx == null)
            createContext();
        Topic dest = null;
        try {
            dest = (Topic) ctx.lookup(topic);
        } catch (Exception e) {
            throw new ObjectFactoryException("Error lookup '" + topic + "': " + e.getMessage(), e);
        }
        return dest;
    }

    public void destroy()
            throws ObjectFactoryException {
        if (ctx == null)
            return;
        try {
            ctx.close();
        } catch (NamingException e) {
            throw new ObjectFactoryException("Error destroying InitialContext: " + e.getMessage(), e);
        }
    }
}

