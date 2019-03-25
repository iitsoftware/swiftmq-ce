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

package com.swiftmq.amqp.v100.generated.messaging.delivery_state;

import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * </p><p>
 * At the target, the rejected outcome is used to indicate that an incoming message is
 * invalid and therefore unprocessable. The rejected outcome when applied to a message will
 * cause the  delivery-count  to be incremented in the header of the rejected message.
 * </p><p>
 * </p><p>
 * At the source, the rejected outcome means that the target has informed the source that
 * the message was rejected, and the source has taken the required action. The delivery
 * SHOULD NOT ever spontaneously attain the rejected state at the source.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Rejected extends AMQPList
        implements DeliveryStateIF, OutcomeIF {
    public static String DESCRIPTOR_NAME = "amqp:rejected:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000025L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    boolean dirty = false;

    Error error = null;

    /**
     * Constructs a Rejected.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Rejected(List initValue) throws Exception {
        super(initValue);
        if (initValue != null)
            decode();
    }

    /**
     * Constructs a Rejected.
     */
    public Rejected() {
        dirty = true;
    }

    /**
     * Return whether this Rejected has a descriptor
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
     * Returns the optional Error field.
     *
     * @return Error
     */
    public Error getError() {
        return error;
    }

    /**
     * Sets the optional Error field.
     *
     * @param error Error
     */
    public void setError(Error error) {
        dirty = true;
        this.error = error;
    }

    /**
     * Returns the predicted size of this Rejected. The predicted size may be greater than the actual size
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
            n += (error != null ? error.getPredictedSize() : 1);
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

        // Field: error
        // Type     : Error, converted: Error
        // Basetype : list
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                error = new Error(((AMQPList) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'error' in 'Rejected' type: " + e);
        }
    }

    private void addToList(List list, Object value) {
        if (value != null)
            list.add(value);
        else
            list.add(AMQPNull.NULL);
    }

    private void encode() throws IOException {
        List l = new ArrayList();
        addToList(l, error);
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
        StringBuffer b = new StringBuffer("[Rejected ");
        b.append(getDisplayString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (error != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("error=");
            b.append(error.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Rejected " + getDisplayString() + "]";
    }
}
