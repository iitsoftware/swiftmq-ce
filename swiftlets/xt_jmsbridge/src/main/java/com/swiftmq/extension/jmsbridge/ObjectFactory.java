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

package com.swiftmq.extension.jmsbridge;

import javax.jms.Queue;
import javax.jms.QueueConnectionFactory;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import java.util.Properties;

/**
 * Factory used from the JMS bridge to create the appropriate
 * JMS administered objects (destinations, connection factories)
 * for the foreign JMS server. The bridge provides an implementation
 * for JNDI lookups. Because this is the standard way of getting
 * administered JMS objects, it should match most cases. If you
 * are in the situation where you have to bridge to a JMS server
 * which doesn't provide a JNDI lookup and where you have to use
 * their proprietary connection factories directly, then you can
 * create your own ObjectFactory by implementing this interface.
 * <BR><BR>
 * The ObjectFactory must be specified while setting up the bridge
 * through the property <tt>objectfactory</tt>. Implementations have
 * to provide a public constructor without parameters. If specified,
 * the bridge will set loaded properties immediatly after creating
 * of the ObjectFactory with <tt>setProperties</tt>.
 *
 * @author IIT GmbH, Bremen/Germany
 * @version 1.0
 * @see JNDIObjectFactory
 **/
public interface ObjectFactory {
    /**
     * Set the properties for this ObjectFactory. A property file can
     * be specified by creating a bridge. If specified, the file will be loaded
     * and set immediatly after creation of the ObjectFactory.
     *
     * @param prop properties
     */
    public void setProperties(Properties prop);

    /**
     * Returns a <tt>QueueConnectionFactory</tt>.
     *
     * @param name Name of the <tt>QueueConnectionFactory</tt>
     * @throws ObjectFactoryException if an error occurs
     */
    public QueueConnectionFactory getQueueConnectionFactory(String name)
            throws ObjectFactoryException;

    /**
     * Returns a <tt>TopicConnectionFactory</tt>.
     *
     * @param name Name of the <tt>TopicConnectionFactory</tt>
     * @throws ObjectFactoryException if an error occurs
     */
    public TopicConnectionFactory getTopicConnectionFactory(String name)
            throws ObjectFactoryException;

    /**
     * Returns a <tt>Queue</tt>.
     *
     * @param name Name of the <tt>Queue</tt>
     * @throws ObjectFactoryException if an error occurs
     */
    public Queue getQueue(String queue)
            throws ObjectFactoryException;

    /**
     * Returns a <tt>Topic</tt>.
     *
     * @param name Name of the <tt>Topic</tt>
     * @throws ObjectFactoryException if an error occurs
     */
    public Topic getTopic(String topic)
            throws ObjectFactoryException;

    /**
     * Destroys the ObjectFactory and frees all resources.
     *
     * @throws ObjectFactoryException if an error occurs
     */
    public void destroy()
            throws ObjectFactoryException;
}

