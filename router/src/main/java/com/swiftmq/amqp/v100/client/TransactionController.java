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

package com.swiftmq.amqp.v100.client;

import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.Rejected;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue;
import com.swiftmq.amqp.v100.generated.transactions.coordination.*;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPBoolean;

import java.util.Set;

/**
 * <p>
 * A transaction controller controls transactions of a session.
 * </p>
 * <p>
 * It communicates withe the transactional resources of the remote server.
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class TransactionController {
    Session mySession;
    Producer producer;
    volatile boolean supportLocalTransactions = true;
    volatile boolean supportDistributedTransactions = false;
    volatile boolean supportPromotableTransactions = false;
    volatile boolean supportMultiTxnsPerSsn = false;
    volatile boolean supportMultiSsnsPerTxn = false;

    protected TransactionController(Session mySession) {
        this.mySession = mySession;
    }

    /**
     * Creates a new transaction id.
     *
     * @return transaction id
     * @throws AMQPException on error
     */
    public synchronized TxnIdIF createTxnId() throws AMQPException {
        if (producer == null) {
            producer = mySession.createProducer(Coordinator.DESCRIPTOR_NAME, QoS.AT_LEAST_ONCE);
            producer.setTransactionController(true);
            Set capa = producer.getDestinationCapabilities();
            if (capa != null) {
                if (capa.contains(TxnCapability.LOCAL_TRANSACTIONS.getValue()))
                    supportLocalTransactions = true;
                supportDistributedTransactions = capa.contains(TxnCapability.DISTRIBUTED_TRANSACTIONS.getValue());
                supportPromotableTransactions = capa.contains(TxnCapability.PROMOTABLE_TRANSACTIONS.getValue());
                supportMultiTxnsPerSsn = capa.contains(TxnCapability.MULTI_TXNS_PER_SSN.getValue());
                supportMultiSsnsPerTxn = capa.contains(TxnCapability.MULTI_SSNS_PER_TXN.getValue());
            }
        }
        AMQPMessage msg = new AMQPMessage();
        msg.setAmqpValue(new AmqpValue(new Declare()));
        Declared declared = (Declared) producer.send(msg);
        return declared == null ? null : declared.getTxnId();
    }

    private synchronized void discharge(TxnIdIF txnId, boolean fail) throws AMQPException {
        AMQPMessage msg = new AMQPMessage();
        Discharge discharge = new Discharge();
        discharge.setTxnId(txnId);
        discharge.setFail(new AMQPBoolean(fail));
        msg.setAmqpValue(new AmqpValue(discharge));
        DeliveryStateIF deliveryState = producer.send(msg);
        if (deliveryState instanceof Rejected) {
            Rejected rejected = (Rejected) deliveryState;
            com.swiftmq.amqp.v100.generated.transport.definitions.Error error = rejected.getError();
            if (error != null)
                throw new AMQPException(error.getValueString());
            else
                throw new AMQPException(("Unknown transactiom error"));
        }
    }

    /**
     * Commits a transaction.
     *
     * @param txnId transaction id.
     * @throws AMQPException on error
     */
    public void commit(TxnIdIF txnId) throws AMQPException {
        discharge(txnId, false);
    }

    /**
     * Rolls back a transaction.
     *
     * @param txnId transaction id.
     * @throws AMQPException on error
     */
    public void rollback(TxnIdIF txnId) throws AMQPException {
        discharge(txnId, true);
    }

    /**
     * Returns whether the attached server-side transactional resource supports local transactions.
     *
     * @return true/false
     */
    public boolean isSupportLocalTransactions() {
        return supportLocalTransactions;
    }

    /**
     * Returns whether the attached server-side transactional resource supports distributed transactions.
     *
     * @return true/false
     */
    public boolean isSupportDistributedTransactions() {
        return supportDistributedTransactions;
    }

    /**
     * Returns whether the attached server-side transactional resource supports promotable transactions.
     *
     * @return true/false
     */
    public boolean isSupportPromotableTransactions() {
        return supportPromotableTransactions;
    }

    /**
     * Returns whether the attached server-side transactional resource supports multiple transactions per session.
     *
     * @return true/false
     */
    public boolean isSupportMultiTxnsPerSsn() {
        return supportMultiTxnsPerSsn;
    }

    /**
     * Returns whether the attached server-side transactional resource supports multiple sessions per transaction.
     *
     * @return true/false
     */
    public boolean isSupportMultiSsnsPerTxn() {
        return supportMultiSsnsPerTxn;
    }

    protected void close() {
        if (producer != null) {
            try {
                producer.close();
            } catch (AMQPException e) {
            }
            producer = null;
        }
    }

    public String toString() {
        return "[TransaactionController, supportLocalTransactions=" + supportLocalTransactions + ", supportDistributedTransactions=" + supportDistributedTransactions + ", supportPromotableTransactions=" + supportPromotableTransactions + ", supportMultiTxnsPerSsn=" + supportMultiTxnsPerSsn + ", supportMultiSsnsPerTxn=" + supportMultiSsnsPerTxn + "]";
    }
}
