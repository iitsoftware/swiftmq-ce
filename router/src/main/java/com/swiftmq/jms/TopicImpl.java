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

package com.swiftmq.jms;

import com.swiftmq.jndi.SwiftMQObjectFactory;
import com.swiftmq.tools.util.LazyUTF8String;

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implementation of a Topic.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class TopicImpl extends QueueImpl
        implements Topic {
    LazyUTF8String topicName;

    /**
     * Creates a new TopicImpl.
     *
     * @param topicName topic name.
     */
    public TopicImpl(String topicName) {
        super(null);
        setTopicName(topicName);
    }

    /**
     * Creates a new TopicImpl.
     *
     * @param queueName name of the topic queue.
     * @param topicName topic name.
     */
    public TopicImpl(String queueName, String topicName) {
        super(queueName);
        setTopicName(topicName);
    }

    /**
     * Creates a new TopicImpl.
     */
    public TopicImpl() {
    }

    public Reference getReference() throws NamingException {
        return new Reference(TopicImpl.class.getName(),
                new StringRefAddr("topicName", topicName != null ? topicName.getString() : null),
                SwiftMQObjectFactory.class.getName(),
                null);
    }

    public int getType() {
        return DestinationFactory.TYPE_TOPIC;
    }

    public void unfoldBuffers() {
        super.unfoldBuffers();
        if (topicName != null)
            topicName.getString(true);
    }

    public void writeContent(DataOutput out) throws IOException {
        if (queueName == null)
            out.writeByte(0);
        else {
            out.writeByte(1);
            queueName.writeContent(out);
        }
        topicName.writeContent(out);
    }

    public void readContent(DataInput in) throws IOException {
        byte set = in.readByte();
        if (set == 1)
            queueName = new LazyUTF8String(in);
        topicName = new LazyUTF8String(in);
    }

    public String getTopicName()
            throws JMSException {
        return topicName != null ? topicName.getString() : null;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName != null ? new LazyUTF8String(topicName) : null;
    }

    public String toString() {
        return topicName != null ? topicName.getString() : null;
    }
}

