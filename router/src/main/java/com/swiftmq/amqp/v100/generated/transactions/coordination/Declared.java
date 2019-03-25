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

package com.swiftmq.amqp.v100.generated.transactions.coordination;

import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateVisitor;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.OutcomeIF;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.OutcomeVisitor;
import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * </p><p>
 * Indicates that a transaction identifier has successfully been allocated in response to a
 * declare message sent to a transaction coordinator.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Declared extends AMQPList
        implements DeliveryStateIF, OutcomeIF {
    public static String DESCRIPTOR_NAME = "amqp:declared:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000033L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    boolean dirty = false;

    TxnIdIF txnId = null;

    /**
     * Constructs a Declared.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Declared(List initValue) throws Exception {
        super(initValue);
        if (initValue != null)
            decode();
    }

    /**
     * Constructs a Declared.
     */
    public Declared() {
        dirty = true;
    }

    /**
     * Return whether this Declared has a descriptor
     *
     * @return true/false
     */
    public boolean hasDescriptor() {
        return true;
    }

    /**
     * Accept method for a DeliveryState visitor.
     *
     * @param visitor DeliveryState visitor
     */
    public void accept(DeliveryStateVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Accept method for a Outcome visitor.
     *
     * @param visitor Outcome visitor
     */
    public void accept(OutcomeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Returns the mandatory TxnId field.
     *
     * @return TxnId
     */
    public TxnIdIF getTxnId() {
        return txnId;
    }

    /**
     * Sets the mandatory TxnId field.
     *
     * @param txnId TxnId
     */
    public void setTxnId(TxnIdIF txnId) {
        dirty = true;
        this.txnId = txnId;
    }

    /**
     * Returns the predicted size of this Declared. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        int n;
        if (dirty) {
            AMQPDescribedConstructor _c = getConstructor();
            setConstructor(null);
            n = super.getPredictedSize();
            n += codeConstructor.getPredictedSize();
            n += (txnId != null ? txnId.getPredictedSize() : 1);
            setConstructor(_c);
        } else
            n = super.getPredictedSize();
        return n;
    }

    private AMQPArray singleToArray(AMQPType t) throws IOException {
        return new AMQPArray(t.getCode(), new AMQPType[]{t});
    }

    private void decode() throws Exception {
        List l = getValue();

        AMQPType t = null;
        int idx = 0;

        // Field: txnId
        // Type     : TxnIdIF, converted: TxnIdIF
        // Basetype : TxnIdIF
        // Default  : null
        // Mandatory: true
        // Multiple : false
        // Factory  : TxnIdFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() == AMQPTypeDecoder.NULL)
            throw new Exception("Mandatory field 'txnId' in 'Declared' type is NULL");
        txnId = TxnIdFactory.create(t);
    }

    private void addToList(List list, Object value) {
        if (value != null)
            list.add(value);
        else
            list.add(AMQPNull.NULL);
    }

    private void encode() throws IOException {
        List l = new ArrayList();
        addToList(l, txnId);
        for (ListIterator iter = l.listIterator(l.size()); iter.hasPrevious(); ) {
            AMQPType t = (AMQPType) iter.previous();
            if (t.getCode() == AMQPTypeDecoder.NULL)
                iter.remove();
            else
                break;
        }
        setValue(l);
        dirty = false;
    }

    /**
     * Returns an array constructor (internal use)
     *
     * @return array constructor
     */
    public AMQPDescribedConstructor getArrayConstructor() throws IOException {
        if (dirty)
            encode();
        codeConstructor.setFormatCode(getCode());
        return codeConstructor;
    }

    public void writeContent(DataOutput out) throws IOException {
        if (dirty)
            encode();
        if (getConstructor() != codeConstructor) {
            codeConstructor.setFormatCode(getCode());
            setConstructor(codeConstructor);
        }
        super.writeContent(out);
    }

    public String getValueString() {
        try {
            if (dirty)
                encode();
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuffer b = new StringBuffer("[Declared ");
        b.append(getDisplayString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (txnId != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("txnId=");
            b.append(txnId.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Declared " + getDisplayString() + "]";
    }
}
