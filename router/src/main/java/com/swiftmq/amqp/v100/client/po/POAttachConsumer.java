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

package com.swiftmq.amqp.v100.client.po;

import com.swiftmq.amqp.v100.client.DeliveryMemory;
import com.swiftmq.amqp.v100.client.SessionVisitor;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POVisitor;

public class POAttachConsumer extends POAttach {
    String source;
    int linkCredit;
    int qoS;
    boolean noLocal;
    String selector;

    public POAttachConsumer(Semaphore semaphore, String source, int linkCredit, int qoS, boolean noLocal, String selector, DeliveryMemory deliveryMemory) {
        super(null, semaphore, deliveryMemory);
        this.source = source;
        this.linkCredit = linkCredit;
        this.qoS = qoS;
        this.noLocal = noLocal;
        this.selector = selector;
    }

    public String getSource() {
        return source;
    }

    public int getLinkCredit() {
        return linkCredit;
    }

    public int getQoS() {
        return qoS;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    public String getSelector() {
        return selector;
    }

    public void accept(POVisitor visitor) {
        ((SessionVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[POAttachConsumer, source=" + source + ", link=" + link + ", linkCredit=" + linkCredit + ", qoS=" + qoS + ", noLocal=" + noLocal + ", selector=" + selector + ", deliveryMemory=" + deliveryMemory + "]";
    }
}