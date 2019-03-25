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

import com.swiftmq.amqp.v100.client.Consumer;
import com.swiftmq.amqp.v100.client.SessionVisitor;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POFillCache extends POObject {
    Consumer consumer;
    int linkCredit;
    long lastDeliveryId;
    TxnIdIF txnIdIF;

    public POFillCache(Consumer consumer, int linkCredit, long lastDeliveryId, TxnIdIF txnIdIF) {
        super(null, null);
        this.consumer = consumer;
        this.linkCredit = linkCredit;
        this.lastDeliveryId = lastDeliveryId;
        this.txnIdIF = txnIdIF;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public int getLinkCredit() {
        return linkCredit;
    }

    public long getLastDeliveryId() {
        return lastDeliveryId;
    }

    public TxnIdIF getTxnIdIF() {
        return txnIdIF;
    }

    public void accept(POVisitor visitor) {
        ((SessionVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[POFillCache, consumer=" + consumer + ", linkCredit=" + linkCredit + ", lastDeliveryId=" + lastDeliveryId + ", txnid=" + txnIdIF + "]";
    }
}