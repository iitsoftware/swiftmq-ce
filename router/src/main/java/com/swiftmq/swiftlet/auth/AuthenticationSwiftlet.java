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

import com.swiftmq.swiftlet.Swiftlet;

/**
 * The AuthenticationSwiftlet is responsible for user authenticating and resource usage verification.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class AuthenticationSwiftlet extends Swiftlet {

    /**
     * Returns a user's password
     *
     * @param userName username
     * @throws AuthenticationException if the user is unknown
     */
    public abstract String getPassword(String userName)
            throws AuthenticationException;

    /**
     * Creates an <code>ActiveLogin</code> object.
     *
     * @param userName username
     * @return active login
     */
    public ActiveLogin createActiveLogin(String userName, String type) {
        ActiveLogin activeLogin = new ActiveLogin(createLoginId(userName), userName, System.currentTimeMillis(), type, createResourceLimitGroup(userName));
        return activeLogin;
    }

    /**
     * Verifies if the user is granted to login from the given host.
     *
     * @param username username
     * @param hostname hostname
     * @throws AuthenticationException if he is not granted
     * @throws ResourceLimitException  if the max number of concurrent connections for this user has been reached
     */
    public abstract void verifyHostLogin(String username, String hostname)
            throws AuthenticationException, ResourceLimitException;

    /**
     * Verifies if the user is granted to set a new client id
     *
     * @param loginId login id
     * @throws AuthenticationException if he is not granted
     */
    public abstract void verifySetClientId(Object loginId)
            throws AuthenticationException;

    /**
     * Verifies if the user is granted to create a queue
     *
     * @param loginId login id
     * @throws AuthenticationException if he is not granted
     */
    public abstract void verifyQueueCreation(Object loginId)
            throws AuthenticationException;

    /**
     * Verifies if the user is granted to subscribe to the queue as a sender
     *
     * @param queueName queue name
     * @param loginId   login id
     * @throws AuthenticationException if he is not granted
     */
    public abstract void verifyQueueSenderSubscription(String queueName, Object loginId)
            throws AuthenticationException;

    /**
     * Verifies if the user is granted to subscribe to the queue as a receiver
     *
     * @param queueName queue name
     * @param loginId   login id
     * @throws AuthenticationException if he is not granted
     */
    public abstract void verifyQueueReceiverSubscription(String queueName, Object loginId)
            throws AuthenticationException;

    /**
     * Verifies if the user is granted to create a browser at the queue
     *
     * @param queueName queue name
     * @param loginId   login id
     * @throws AuthenticationException if he is not granted
     */
    public abstract void verifyQueueBrowserCreation(String queueName, Object loginId)
            throws AuthenticationException;

    /**
     * Verifies if the user is granted to subscribe to a topic as a sender
     *
     * @param topicName topic name
     * @param loginId   login id
     * @throws AuthenticationException if he is not granted
     */
    public abstract void verifyTopicSenderSubscription(String topicName, Object loginId)
            throws AuthenticationException;

    /**
     * Verifies if the user is granted to subscribe to a topic as a receiver
     *
     * @param topicName topic name
     * @param loginId   login id
     * @throws AuthenticationException if he is not granted
     */
    public abstract void verifyTopicReceiverSubscription(String topicName, Object loginId)
            throws AuthenticationException;

    /**
     * Verifies if the user is granted to create a durable subscriber at the topic
     *
     * @param topicName topic name
     * @param loginId   login id
     * @throws AuthenticationException if he is not granted
     */
    public abstract void verifyTopicDurableSubscriberCreation(String topicName, Object loginId)
            throws AuthenticationException;

    public abstract void addTopicAuthenticationDelegate(AuthenticationDelegate delegate);

    public abstract void removeTopicAuthenticationDelegate(AuthenticationDelegate delegate);

    /**
     * Log out of a user
     *
     * @param userName user name
     * @param loginId  login id
     */
    public abstract void logout(String userName, Object loginId);


    /**
     * Creates a unique login id.
     *
     * @param userName user name
     * @return login id
     */
    protected abstract Object createLoginId(String userName);

    /**
     * Creates a resource limit group
     *
     * @param userName user name
     * @return resource limit group
     */
    protected abstract ResourceLimitGroup createResourceLimitGroup(String userName);
}

