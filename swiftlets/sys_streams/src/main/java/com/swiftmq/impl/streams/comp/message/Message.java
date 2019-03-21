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

package com.swiftmq.impl.streams.comp.message;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * Facade to wrap a javax.jms.Message.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */

public class Message {
    StreamContext ctx;
    MessageImpl _impl;
    boolean onMessageEnabled = true;
    boolean isLocalCopy = false;

    Message(StreamContext ctx) {
        this.ctx = ctx;
        _impl = new MessageImpl();
    }

    Message(StreamContext ctx, MessageImpl _impl) {
        this.ctx = ctx;
        this._impl = _impl;
    }

    /**
     * Internal use only.
     */
    public MessageImpl getImpl() {
        return _impl;
    }

    private MessageImpl copyMessage(MessageImpl msg) throws Exception {
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream(4096);
        DataByteArrayInputStream dbis = new DataByteArrayInputStream();
        dbos.rewind();
        msg.writeContent(dbos);
        dbis.reset();
        dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
        MessageImpl msgCopy = MessageImpl.createInstance(dbis.readInt());
        msgCopy.readContent(dbis);
        return msgCopy;
    }

    void ensureLocalCopy() {
        try {
            if (!isLocalCopy) {
                _impl = copyMessage(_impl);
                isLocalCopy = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Property addProperty(Property prop) {
        ensureLocalCopy();
        try {
            if (prop.name().equals(MessageImpl.PROP_CLIENT_ID))
                _impl.setStringProperty(prop.name(), prop.value().value().toString());
            else
                _impl.setObjectProperty(prop.name(), prop.value().value());
        } catch (JMSException e) {

        }
        return prop;
    }

    Property removeProperty(Property prop) {
        ensureLocalCopy();
        _impl.removeProperty(prop.name());
        return prop;
    }

    /**
     * Internal use only.
     */
    public Message copyProperties(Message source) throws JMSException {
        ensureLocalCopy();
        MessageImpl sourceImpl = source.getImpl();
        for (Enumeration _e = sourceImpl.getPropertyNames(); _e.hasMoreElements(); ) {
            String name = (String) _e.nextElement();
            _impl.setObjectProperty(name, sourceImpl.getObjectProperty(name));
        }
        return this;
    }

    /**
     * Internal use only.
     */
    public boolean isSelected(MessageSelector selector) throws JMSException {
        return selector.isSelected(_impl);
    }

    /**
     * return the type of this Message
     *
     * @return "message"
     */
    public String type() {
        return "message";
    }

    /**
     * Marks this Message as persistent.
     *
     * @return Message
     * @throws JMSException
     */
    public Message persistent() throws JMSException {
        _impl.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
        return this;
    }

    /**
     * Returns true if this Message is persistent.
     *
     * @return true/false
     * @throws JMSException
     */
    public boolean isPersistent() throws JMSException {
        return _impl.getJMSDeliveryMode() == DeliveryMode.PERSISTENT;
    }

    /**
     * Marks this Message as non-persistent.
     *
     * @return Message
     * @throws JMSException
     */
    public Message nonpersistent() throws JMSException {
        _impl.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return this;
    }

    /**
     * Returns true if this Message is non-persistent.
     *
     * @return true/false
     * @throws JMSException
     */
    public boolean isNonpersistent() throws JMSException {
        return _impl.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT;
    }

    /**
     * Returns the Name of the JMSDestination.
     *
     * @return Name of the JMSDestination
     * @throws JMSException
     */
    public String destination() throws JMSException {
        Destination dest = _impl.getJMSDestination();
        if (dest == null)
            return null;
        if (dest instanceof TopicImpl)
            return ((TopicImpl) dest).getTopicName();
        return ((QueueImpl) dest).getQueueName();
    }

    /**
     * Returns the size (bytes) of the Message.
     *
     * @return size
     */
    public long size() {
        return _impl.getMessageLength();
    }

    /**
     * Sets the JMS message ID.
     *
     * @param s message id
     * @return Message
     * @throws JMSException
     */
    public Message messageId(String s) throws JMSException {
        _impl.setJMSMessageID(s);
        return this;
    }

    /**
     * Returns the JMS message ID.
     *
     * @return message id
     * @throws JMSException
     */
    public String messageId() throws JMSException {
        return _impl.getJMSMessageID();
    }

    /**
     * Sets the JMS correlation ID.
     *
     * @param s correlation id
     * @return Message
     * @throws JMSException
     */
    public Message correlationId(String s) throws JMSException {
        _impl.setJMSCorrelationID(s);
        return this;
    }

    /**
     * Returns the JMS correlation ID.
     *
     * @return correlation id
     * @throws JMSException
     */
    public String correlationId() throws JMSException {
        return _impl.getJMSCorrelationID();
    }

    /**
     * Sets the JMS priority.
     *
     * @param prio priority
     * @return Message
     * @throws JMSException
     */
    public Message priority(int prio) throws JMSException {
        _impl.setJMSPriority(prio);
        return this;
    }

    /**
     * Returns the JMS priority
     *
     * @return priority
     * @throws JMSException
     */
    public int priority() throws JMSException {
        return _impl.getJMSPriority();
    }

    /**
     * Sets the JMS expiration
     *
     * @param exp expiration
     * @return Message
     * @throws JMSException
     */
    public Message expiration(long exp) throws JMSException {
        _impl.setJMSExpiration(exp);
        return this;
    }

    /**
     * Returns the JMS expiration.
     *
     * @return expiration
     * @throws JMSException
     */
    public long expiration() throws JMSException {
        return _impl.getJMSExpiration();
    }

    /**
     * Sets the JMS timestamp
     *
     * @param timestamp time stamp
     * @return Message
     * @throws JMSException
     */
    public Message timestamp(long timestamp) throws JMSException {
        _impl.setJMSTimestamp(timestamp);
        return this;
    }

    /**
     * Returns the JMS timestamp.
     *
     * @return timestamp
     * @throws JMSException
     */
    public long timestamp() throws JMSException {
        return _impl.getJMSTimestamp();
    }

    /**
     * Sets the replyTo address
     *
     * @param destination
     * @return Message
     * @throws JMSException
     */
    public Message replyTo(Destination destination) throws JMSException {
        _impl.setJMSReplyTo(destination);
        return this;
    }

    /**
     * Returns the replyTo address
     *
     * @return replyTo Address
     * @throws JMSException
     */
    public Destination replyTo() throws JMSException {
        return _impl.getJMSReplyTo();
    }

    /**
     * Returns the Message property with that name. If the property does not
     * exists in the message, the value of the returned property is set to null.
     *
     * @param name Property name
     * @return Property
     */
    public Property property(String name) {
        Property prop = null;
        try {
            prop = new Property(this, name, _impl.getObjectProperty(name));
        } catch (JMSException e) {
        }
        return prop;
    }

    /**
     * Clears the message body.
     *
     * @return this
     */
    public Message clearBody() {
        ensureLocalCopy();
        try {
            _impl.clearBody();
        } catch (JMSException e) {
        }
        return this;
    }

    /**
     * Returns a PropertySet that contains all Message Properties of this Message
     *
     * @return PropertySet
     * @throws Exception
     */
    public PropertySet properties() throws Exception {
        Map<String, Property> props = new HashMap<String, Property>();
        for (Enumeration _enum = _impl.getPropertyNames(); _enum.hasMoreElements(); ) {
            String name = (String) _enum.nextElement();
            props.put(name, new Property(this, name, _impl.getObjectProperty(name)));
        }
        return new PropertySet(this, props);
    }

    /**
     * Can be set to false in an Input callback to disable calling the onMessage Callback with this Message.
     *
     * @param onMessageEnabled true/false
     * @return this
     */
    public Message onMessageEnabled(boolean onMessageEnabled) {
        this.onMessageEnabled = onMessageEnabled;
        return this;
    }

    /**
     * Internal use.
     */
    public boolean isOnMessageEnabled() {
        return onMessageEnabled;
    }

    @Override
    public String toString() {
        return "Message{" +
                "_impl=" + _impl +
                '}';
    }
}
