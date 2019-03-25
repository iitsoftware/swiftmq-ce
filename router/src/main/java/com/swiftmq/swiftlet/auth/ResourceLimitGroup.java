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

package com.swiftmq.swiftlet.auth;

/**
 * A ResourceLimitGroup contains the maximum values for connections, sessions per connection,
 * temp. queues/topics per connection, producers and consumers per connection a user can obtain from a SwiftMQ router.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2004, All Rights Reserved
 */
public class ResourceLimitGroup {
    String name = null;
    int maxConnections;
    int maxSessions;
    int maxTempQueues;
    int maxProducers;
    int maxConsumers;
    int sessions = 0;
    int tempQueues = 0;
    int producers = 0;
    int consumers = 0;

    /**
     * Creates a new ResourceLimitGroup.
     *
     * @param name           Name of the group
     * @param maxConnections max. connections
     * @param maxSessions    max. sessions
     * @param maxTempQueues  max. temp. queues/topics
     * @param maxProducers   max. producers
     * @param maxConsumers   max. consumers
     */
    public ResourceLimitGroup(String name, int maxConnections, int maxSessions, int maxTempQueues, int maxProducers, int maxConsumers) {
        this.name = name;
        this.maxConnections = maxConnections;
        this.maxSessions = maxSessions;
        this.maxTempQueues = maxTempQueues;
        this.maxProducers = maxProducers;
        this.maxConsumers = maxConsumers;
    }

    /**
     * Returns the group name
     *
     * @return group name
     */
    public String getName() {
        return (name);
    }

    /**
     * Returns the max. connections
     *
     * @return max. connections
     */
    public synchronized int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Set the max. sessions
     *
     * @param maxConnections max. connections
     */
    public synchronized void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public synchronized void verifyConnectionLimit(int n) throws ResourceLimitException {
        if (maxConnections != -1 && n >= maxConnections)
            throw new ResourceLimitException("Resource Limit Group '" + name + "': max connections exceeded. Resource limit is: " + maxConnections);
    }

    /**
     * Returns the max. sessions
     *
     * @return max. sessions
     */
    public synchronized int getMaxSessions() {
        return (maxSessions);
    }

    /**
     * Set the max. sessions
     *
     * @param maxSessions max. sessions
     */
    public synchronized void setMaxSessions(int maxSessions) {
        this.maxSessions = maxSessions;
    }

    /**
     * Returns the max. temp queues/topics
     *
     * @return max. temp queues/topics
     */
    public synchronized int getMaxTempQueues() {
        return (maxTempQueues);
    }

    /**
     * Set the max. temp queues/topics
     *
     * @param maxTempQueues max. temp queues/topics
     */
    public synchronized void setMaxTempQueues(int maxTempQueues) {
        this.maxTempQueues = maxTempQueues;
    }

    /**
     * Returns the max. producers
     *
     * @return max. producers
     */
    public synchronized int getMaxProducers() {
        return (maxProducers);
    }

    /**
     * Set the max. producers
     *
     * @param maxProducers max. producers
     */
    public synchronized void setMaxProducers(int maxProducers) {
        this.maxProducers = maxProducers;
    }

    /**
     * Returns the max. consumers
     *
     * @return max. consumers
     */
    public synchronized int getMaxConsumers() {
        return (maxConsumers);
    }

    /**
     * Set the max. consumers
     *
     * @param maxConsumers max. consumers
     */
    public synchronized void setMaxConsumers(int maxConsumers) {
        this.maxConsumers = maxConsumers;
    }

    /**
     * Increments the number of sessions in use
     *
     * @throws ResourceLimitException if max. sessions is exceeded
     */
    public synchronized void incSessions()
            throws ResourceLimitException {
        if (sessions >= maxSessions)
            throw new ResourceLimitException("Resource Limit Group '" + name + "': max sessions per connection exceeded. Resource limit is: " + maxSessions);
        sessions++;
    }

    /**
     * Decrements the number of sessions in use
     */
    public synchronized void decSessions() {
        sessions--;
        if (sessions < 0)
            sessions = 0;
    }

    /**
     * Returns the number of sessions in use
     *
     * @return number of sessions in use
     */
    public synchronized int getSessions() {
        return (sessions);
    }

    /**
     * Increments the number of temp. queues/topics in use
     *
     * @throws ResourceLimitException if max. temp. queues/topics is exceeded
     */
    public synchronized void incTempQueues()
            throws ResourceLimitException {
        if (tempQueues >= maxTempQueues)
            throw new ResourceLimitException("Resource Limit Group '" + name + "': max temp. queues per connection exceeded. Resource limit is: " + maxTempQueues);
        tempQueues++;
    }

    /**
     * Decrements the number of temp. queues/topics in use
     */
    public synchronized void decTempQueues() {
        tempQueues--;
        if (tempQueues < 0)
            tempQueues = 0;
    }

    /**
     * Returns the number of temp. queues/topics in use
     *
     * @return number of temp. queues/topics in use
     */
    public synchronized int getTempQueues() {
        return (tempQueues);
    }

    /**
     * Increments the number of producers in use
     *
     * @throws ResourceLimitException if max. producers is exceeded
     */
    public synchronized void incProducers()
            throws ResourceLimitException {
        if (producers >= maxProducers)
            throw new ResourceLimitException("Resource Limit Group '" + name + "': max producers per connection exceeded. Resource limit is: " + maxProducers);
        producers++;
    }

    /**
     * Decrements the number of producers in use
     */
    public synchronized void decProducers() {
        producers--;
        if (producers < 0)
            producers = 0;
    }

    /**
     * Returns the number of producers in use
     *
     * @return number of producers in use
     */
    public synchronized int getProducers() {
        return (producers);
    }

    /**
     * Increments the number of consumers in use
     *
     * @throws ResourceLimitException if max. consumers is exceeded
     */
    public synchronized void incConsumers()
            throws ResourceLimitException {
        if (consumers >= maxConsumers)
            throw new ResourceLimitException("Resource Limit Group '" + name + "': max consumers per connection exceeded. Resource limit is: " + maxConsumers);
        consumers++;
    }

    /**
     * Decrements the number of consumers in use
     */
    public synchronized void decConsumers() {
        consumers--;
        if (consumers < 0)
            consumers = 0;
    }

    /**
     * Returns the number of consumers in use
     *
     * @return number of consumers in use
     */
    public synchronized int getConsumers() {
        return (consumers);
    }

    public String toString() {
        StringBuffer b = new StringBuffer("[ResourceLimitGroup ");
        b.append(name);
        b.append(", maxConnections=");
        b.append(maxConnections);
        b.append(", maxSessions=");
        b.append(maxSessions);
        b.append(", sessions=");
        b.append(sessions);
        b.append(", maxTempQueues=");
        b.append(maxTempQueues);
        b.append(", tempQueues=");
        b.append(tempQueues);
        b.append(", maxProducers=");
        b.append(maxProducers);
        b.append(", producers=");
        b.append(producers);
        b.append(", maxConsumers=");
        b.append(maxConsumers);
        b.append(", consumers=");
        b.append(consumers);
        b.append("]");
        return b.toString();
    }
}

