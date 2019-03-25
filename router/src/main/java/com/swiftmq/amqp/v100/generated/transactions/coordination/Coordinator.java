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

import com.swiftmq.amqp.v100.generated.messaging.addressing.TargetIF;
import com.swiftmq.amqp.v100.generated.messaging.addressing.TargetVisitor;
import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * </p><p>
 * The coordinator type defines a special target used for establishing a link with a
 * transaction coordinator.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Coordinator extends AMQPList
        implements TargetIF {
    public static String DESCRIPTOR_NAME = "amqp:coordinator:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000030L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    boolean dirty = false;

    AMQPArray capabilities = null;

    /**
     * Constructs a Coordinator.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Coordinator(List initValue) throws Exception {
        super(initValue);
        if (initValue != null)
            decode();
    }

    /**
     * Constructs a Coordinator.
     */
    public Coordinator() {
        dirty = true;
    }

    /**
     * Return whether this Coordinator has a descriptor
     *
     * @return true/false
     */
    public boolean hasDescriptor() {
        return true;
    }

    /**
     * Accept method for a Target visitor.
     *
     * @param visitor Target visitor
     */
    public void accept(TargetVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Returns the optional Capabilities field.
     *
     * @return Capabilities
     */
    public AMQPArray getCapabilities() {
        return capabilities;
    }

    /**
     * Sets the optional Capabilities field.
     *
     * @param capabilities Capabilities
     */
    public void setCapabilities(AMQPArray capabilities) {
        dirty = true;
        this.capabilities = capabilities;
    }

    /**
     * Returns the predicted size of this Coordinator. The predicted size may be greater than the actual size
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
            n += (capabilities != null ? capabilities.getPredictedSize() : 1);
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

        // Field: capabilities
        // Type     : TxnCapabilityIF, converted: TxnCapabilityIF
        // Basetype : TxnCapabilityIF
        // Default  : null
        // Mandatory: false
        // Multiple : true
        // Factory  : TxnCapabilityFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() != AMQPTypeDecoder.NULL)
            capabilities = AMQPTypeDecoder.isArray(t.getCode()) ? (AMQPArray) t : singleToArray(t);
    }

    private void addToList(List list, Object value) {
        if (value != null)
            list.add(value);
        else
            list.add(AMQPNull.NULL);
    }

    private void encode() throws IOException {
        List l = new ArrayList();
        addToList(l, capabilities);
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
        StringBuffer b = new StringBuffer("[Coordinator ");
        b.append(getDisplayString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (capabilities != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("capabilities=");
            b.append(capabilities.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Coordinator " + getDisplayString() + "]";
    }
}
