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

package com.swiftmq.swiftlet.topic;

import com.swiftmq.jms.TopicImpl;
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.queue.QueueAlreadyDefinedException;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.Selector;
import com.swiftmq.swiftlet.queue.UnknownQueueException;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import java.util.StringTokenizer;

/**
 * The TopicManager manages topics. A topic can be hierarchical, e.g.
 * 'iit.sales.eu'. Every topic has an associated queue which is returned
 * on <code>getQueueForTopic()</code>. This queue is used to publish messages
 * inside of queue transactions. Subsciptions are mapped either to
 * temporary queues (non-durable) or a durable subscriber queue.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class TopicManager extends Swiftlet {

    /**
     * Returns the prefix for topic queues, e.g. 'tpc$'.
     *
     * @return prefix.
     */
    protected abstract String getTopicQueuePrefix();

    /**
     * Returns the delimiter for hierarchical topics, e.g. '.'
     * Additional verbose description.
     *
     * @return delimiter.
     */
    protected abstract String getTopicDelimiter();

    /**
     * Creates a topic.
     *
     * @param topicName topic name.
     * @throws TopicException on error.
     */
    public abstract void createTopic(String topicName) throws TopicException;

    /**
     * Deletes a topic.
     *
     * @param topicName topic name.
     * @throws TopicException on error.
     */
    public abstract void deleteTopic(String topicName) throws TopicException;

    /**
     * Returns whether a topic with that name is defined or not.
     *
     * @param topicName topic name.
     * @return true/false.
     */
    public abstract boolean isTopicDefined(String topicName);

    /**
     * Verifies whether the topic is defined and accessible.
     *
     * @param topic topic.
     * @return true/false.
     * @throws JMSException                on error.
     * @throws InvalidDestinationException if the topic is invalid.
     */
    public abstract TopicImpl verifyTopic(TopicImpl topic) throws JMSException, InvalidDestinationException;

    /**
     * Returns the name of the topic queue for publish.
     *
     * @param topicName topic name.
     * @return queue name.
     */
    public String getQueueForTopic(String topicName) {
        String[] names = tokenizeTopicName(topicName, getTopicDelimiter());
        String name = null;
        if (names != null && names.length > 0)
            name = getTopicQueuePrefix() + names[0];
        return name;
    }

    /**
     * Tokenizes a hierachical topic name by the delimiter.
     * For example, 'iit.sales.eu' with delimiter '.' will return String[]{'iit','sales','eu'}.
     *
     * @param topicName topic name.
     * @param delimiter delimiter.
     * @return tokenized array.
     */
    public String[] tokenizeTopicName(String topicName, String delimiter) {
        StringTokenizer t = new StringTokenizer(topicName, delimiter);
        String[] rarr = new String[t.countTokens()];
        int i = 0;
        while (t.hasMoreTokens())
            rarr[i++] = t.nextToken();
        return rarr;
    }

    /**
     * Tokenizes a hierachical topic name by the default delimiter.
     * For example, 'iit.sales.eu'  will return String[]{'iit','sales','eu'}.
     *
     * @param topicName topic name.
     * @param delimiter delimiter.
     * @return tokenized array.
     */
    public String[] tokenizeTopicName(String topicName) {
        return tokenizeTopicName(topicName, getTopicDelimiter());
    }

    /**
     * Subscribes to a topic.
     *
     * @param topicName topic name.
     * @param selector  selector or null.
     * @param noLocal   no-local flag.
     * @param queueName subscriber queue name.
     * @return subscription id.
     * @throws AuthenticationException on authentication error.
     */
    public int subscribe(String topicName, Selector selector, boolean noLocal, String queueName)
            throws AuthenticationException {
        TopicImpl topic = new TopicImpl(getQueueForTopic(topicName), topicName);
        return subscribe(topic, selector, noLocal, queueName, null);
    }

    /**
     * Subscribes to a topic.
     *
     * @param topicName   topic name.
     * @param selector    selector or null.
     * @param noLocal     no-local flag.
     * @param activeLogin active login object.
     * @return subscription id.
     * @throws AuthenticationException on authentication error.
     */
    public int subscribe(String topicName, Selector selector, boolean noLocal, String queueName, ActiveLogin activeLogin)
            throws AuthenticationException {
        TopicImpl topic = new TopicImpl(getQueueForTopic(topicName), topicName);
        return subscribe(topic, selector, noLocal, queueName, activeLogin);
    }

    /**
     * Subscribes to a topic.
     *
     * @param topicName   topic.
     * @param selector    selector or null.
     * @param noLocal     no-local flag.
     * @param activeLogin active login object.
     * @return subscription id.
     * @throws AuthenticationException on authentication error.
     */
    public abstract int subscribe(TopicImpl topic, Selector selector, boolean noLocal, String queueName, ActiveLogin activeLogin)
            throws AuthenticationException;

    /**
     * Subscribe as durable subscriber.
     *
     * @param durableName durable name.
     * @param topic       topic.
     * @param selector    selector or null.
     * @param noLocal     no-local flag.
     * @param activeLogin active login object.
     * @return durable subscriber queue name.
     * @throws AuthenticationException      on authentication error.
     * @throws QueueException               queue manager exception.
     * @throws QueueAlreadyDefinedException queue manager exception.
     * @throws UnknownQueueException        queue manager exception.
     * @throws TopicException               on topic manager exception.
     */
    public abstract String subscribeDurable(String durableName, TopicImpl topic, Selector selector, boolean noLocal, ActiveLogin activeLogin)
            throws AuthenticationException, QueueException, QueueAlreadyDefinedException, UnknownQueueException, TopicException;

    /**
     * Delete a durable subscriber.
     * Deletes not only the subscription but also the durable subscriber queue.
     *
     * @param durableName durable name.
     * @param activeLogin active login object.
     * @throws InvalidDestinationException queue manager exception.
     * @throws QueueException              queue manager exception.
     * @throws UnknownQueueException       queue manager exception.
     * @throws TopicException              on topic manager exception.
     */
    public abstract void deleteDurable(String durableName, ActiveLogin activeLogin)
            throws InvalidDestinationException, QueueException, UnknownQueueException, TopicException;

    /**
     * Returns the topic name for which the durable subscriber is subcribed
     *
     * @param durableName durable name.
     * @param activeLogin active login object.
     * @return topic name or null if the durable is not found
     */
    public abstract String getDurableTopicName(String durableName, ActiveLogin activeLogin);

    /**
     * Unsubscribe a non-durable subscription.
     *
     * @param subscriberId subscriber id.
     */
    public abstract void unsubscribe(int subscriberId);

    /**
     * Returns all defined topic names.
     *
     * @return array of topic names.
     */
    public abstract String[] getTopicNames();

}

