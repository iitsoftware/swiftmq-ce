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

import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * </p><p>
 * At the target the   state indicates the furthest point in the
 * payload of the message which the target will not need to have resent if the link is
 * resumed. At the source the   state represents the earliest point in
 * the payload which the sender is able to resume transferring at in the case of link
 * resumption. When resuming a delivery, if this state is set on the first   performative it indicates the offset in the payload at which the first
 * resumed delivery is starting. The sender MUST NOT send the   state
 * on   or
 * performatives except on the first   performative on a
 * resumed delivery.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Received extends AMQPList
        implements DeliveryStateIF {
    public static String DESCRIPTOR_NAME = "amqp:received:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000023L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    boolean dirty = false;

    AMQPUnsignedInt sectionNumber = null;
    AMQPUnsignedLong sectionOffset = null;

    /**
     * Constructs a Received.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Received(List initValue) throws Exception {
        super(initValue);
        if (initValue != null)
            decode();
    }

    /**
     * Constructs a Received.
     */
    public Received() {
        dirty = true;
    }

    /**
     * Return whether this Received has a descriptor
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
     * Returns the mandatory SectionNumber field.
     *
     * @return SectionNumber
     */
    public AMQPUnsignedInt getSectionNumber() {
        return sectionNumber;
    }

    /**
     * Sets the mandatory SectionNumber field.
     *
     * @param sectionNumber SectionNumber
     */
    public void setSectionNumber(AMQPUnsignedInt sectionNumber) {
        dirty = true;
        this.sectionNumber = sectionNumber;
    }

    /**
     * Returns the mandatory SectionOffset field.
     *
     * @return SectionOffset
     */
    public AMQPUnsignedLong getSectionOffset() {
        return sectionOffset;
    }

    /**
     * Sets the mandatory SectionOffset field.
     *
     * @param sectionOffset SectionOffset
     */
    public void setSectionOffset(AMQPUnsignedLong sectionOffset) {
        dirty = true;
        this.sectionOffset = sectionOffset;
    }

    /**
     * Returns the predicted size of this Received. The predicted size may be greater than the actual size
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
            n += (sectionNumber != null ? sectionNumber.getPredictedSize() : 1);
            n += (sectionOffset != null ? sectionOffset.getPredictedSize() : 1);
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

        // Field: sectionNumber
        // Type     : uint, converted: AMQPUnsignedInt
        // Basetype : uint
        // Default  : null
        // Mandatory: true
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() == AMQPTypeDecoder.NULL)
            throw new Exception("Mandatory field 'sectionNumber' in 'Received' type is NULL");
        try {
            sectionNumber = (AMQPUnsignedInt) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'sectionNumber' in 'Received' type: " + e);
        }

        // Field: sectionOffset
        // Type     : ulong, converted: AMQPUnsignedLong
        // Basetype : ulong
        // Default  : null
        // Mandatory: true
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() == AMQPTypeDecoder.NULL)
            throw new Exception("Mandatory field 'sectionOffset' in 'Received' type is NULL");
        try {
            sectionOffset = (AMQPUnsignedLong) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'sectionOffset' in 'Received' type: " + e);
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
        addToList(l, sectionNumber);
        addToList(l, sectionOffset);
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
        StringBuffer b = new StringBuffer("[Received ");
        b.append(getDisplayString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (sectionNumber != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("sectionNumber=");
            b.append(sectionNumber.getValueString());
        }
        if (sectionOffset != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("sectionOffset=");
            b.append(sectionOffset.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Received " + getDisplayString() + "]";
    }
}
