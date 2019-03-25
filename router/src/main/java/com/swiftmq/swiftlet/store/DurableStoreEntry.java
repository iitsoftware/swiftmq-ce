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

package com.swiftmq.swiftlet.store;

/**
 * Durable subscriber entry in the DurableSubscriberStore.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class DurableStoreEntry {
    String clientId;
    String durableName;
    String topicName;
    String selector;
    boolean noLocal;


    /**
     * Creates a DurableStoreEntry.
     *
     * @param clientId    client id.
     * @param durableName durable name.
     * @param topicName   topic name.
     * @param selector    selector string.
     * @param noLocal     no-local flaf.
     * @return description.
     */
    public DurableStoreEntry(String clientId, String durableName, String topicName, String selector, boolean noLocal) {
        // SBgen: Assign variables
        this.clientId = clientId;
        this.durableName = durableName;
        this.topicName = topicName;
        this.selector = selector;
        this.noLocal = noLocal;
        // SBgen: End assign
    }


    /**
     * Returns the client id.
     *
     * @return client id.
     */
    public String getClientId() {
        // SBgen: Get variable
        return (clientId);
    }


    /**
     * Returns the durable name.
     *
     * @return durable name.
     */
    public String getDurableName() {
        // SBgen: Get variable
        return (durableName);
    }


    /**
     * Returns the topic name.
     *
     * @return topic name.
     */
    public String getTopicName() {
        // SBgen: Get variable
        return (topicName);
    }


    /**
     * Returns the selector string.
     *
     * @return selector string.
     */
    public String getSelector() {
        // SBgen: Get variable
        return (selector);
    }


    /**
     * Returns the no-local flag.
     *
     * @return no-local flag.
     */
    public boolean isNoLocal() {
        // SBgen: Get variable
        return (noLocal);
    }
}

