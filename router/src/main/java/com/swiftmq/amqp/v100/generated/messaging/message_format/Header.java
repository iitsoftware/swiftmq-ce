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

package com.swiftmq.amqp.v100.generated.messaging.message_format;

import com.swiftmq.amqp.v100.generated.transport.definitions.Milliseconds;
import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * </p><p>
 * The header section carries standard delivery details about the transfer of a message
 * through the AMQP network. If the header section is omitted the receiver MUST assume the
 * appropriate default values (or the meaning implied by no value being set) for the fields
 * within the   unless other target or node specific defaults have
 * otherwise been set.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Header extends AMQPList
        implements SectionIF {
    public static String DESCRIPTOR_NAME = "amqp:header:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000070L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    boolean dirty = false;

    AMQPBoolean durable = AMQPBoolean.FALSE;
    AMQPUnsignedByte priority = new AMQPUnsignedByte(4);
    Milliseconds ttl = null;
    AMQPBoolean firstAcquirer = AMQPBoolean.FALSE;
    AMQPUnsignedInt deliveryCount = new AMQPUnsignedInt(0L);

    /**
     * Constructs a Header.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Header(List initValue) throws Exception {
        super(initValue);
        if (initValue != null)
            decode();
    }

    /**
     * Constructs a Header.
     */
    public Header() {
        dirty = true;
    }

    /**
     * Return whether this Header has a descriptor
     *
     * @return true/false
     */
    public boolean hasDescriptor() {
        return true;
    }

    /**
     * Accept method for a Section visitor.
     *
     * @param visitor Section visitor
     */
    public void accept(SectionVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Returns the optional Durable field.
     *
     * @return Durable
     */
    public AMQPBoolean getDurable() {
        return durable;
    }

    /**
     * Sets the optional Durable field.
     *
     * @param durable Durable
     */
    public void setDurable(AMQPBoolean durable) {
        dirty = true;
        this.durable = durable;
    }

    /**
     * Returns the optional Priority field.
     *
     * @return Priority
     */
    public AMQPUnsignedByte getPriority() {
        return priority;
    }

    /**
     * Sets the optional Priority field.
     *
     * @param priority Priority
     */
    public void setPriority(AMQPUnsignedByte priority) {
        dirty = true;
        this.priority = priority;
    }

    /**
     * Returns the optional Ttl field.
     *
     * @return Ttl
     */
    public Milliseconds getTtl() {
        return ttl;
    }

    /**
     * Sets the optional Ttl field.
     *
     * @param ttl Ttl
     */
    public void setTtl(Milliseconds ttl) {
        dirty = true;
        this.ttl = ttl;
    }

    /**
     * Returns the optional FirstAcquirer field.
     *
     * @return FirstAcquirer
     */
    public AMQPBoolean getFirstAcquirer() {
        return firstAcquirer;
    }

    /**
     * Sets the optional FirstAcquirer field.
     *
     * @param firstAcquirer FirstAcquirer
     */
    public void setFirstAcquirer(AMQPBoolean firstAcquirer) {
        dirty = true;
        this.firstAcquirer = firstAcquirer;
    }

    /**
     * Returns the optional DeliveryCount field.
     *
     * @return DeliveryCount
     */
    public AMQPUnsignedInt getDeliveryCount() {
        return deliveryCount;
    }

    /**
     * Sets the optional DeliveryCount field.
     *
     * @param deliveryCount DeliveryCount
     */
    public void setDeliveryCount(AMQPUnsignedInt deliveryCount) {
        dirty = true;
        this.deliveryCount = deliveryCount;
    }

    /**
     * Returns the predicted size of this Header. The predicted size may be greater than the actual size
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
            n += (durable != null ? durable.getPredictedSize() : 1);
            n += (priority != null ? priority.getPredictedSize() : 1);
            n += (ttl != null ? ttl.getPredictedSize() : 1);
            n += (firstAcquirer != null ? firstAcquirer.getPredictedSize() : 1);
            n += (deliveryCount != null ? deliveryCount.getPredictedSize() : 1);
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

        // Field: durable
        // Type     : boolean, converted: AMQPBoolean
        // Basetype : boolean
        // Default  : AMQPBoolean.FALSE
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                durable = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'durable' in 'Header' type: " + e);
        }

        // Field: priority
        // Type     : ubyte, converted: AMQPUnsignedByte
        // Basetype : ubyte
        // Default  : 4
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                priority = (AMQPUnsignedByte) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'priority' in 'Header' type: " + e);
        }

        // Field: ttl
        // Type     : Milliseconds, converted: Milliseconds
        // Basetype : uint
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                ttl = new Milliseconds(((AMQPUnsignedInt) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'ttl' in 'Header' type: " + e);
        }

        // Field: firstAcquirer
        // Type     : boolean, converted: AMQPBoolean
        // Basetype : boolean
        // Default  : AMQPBoolean.FALSE
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                firstAcquirer = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'firstAcquirer' in 'Header' type: " + e);
        }

        // Field: deliveryCount
        // Type     : uint, converted: AMQPUnsignedInt
        // Basetype : uint
        // Default  : 0
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                deliveryCount = (AMQPUnsignedInt) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'deliveryCount' in 'Header' type: " + e);
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
        addToList(l, durable);
        addToList(l, priority);
        addToList(l, ttl);
        addToList(l, firstAcquirer);
        addToList(l, deliveryCount);
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
        StringBuffer b = new StringBuffer("[Header ");
        b.append(getDisplayString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (durable != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("durable=");
            b.append(durable.getValueString());
        }
        if (priority != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("priority=");
            b.append(priority.getValueString());
        }
        if (ttl != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("ttl=");
            b.append(ttl.getValueString());
        }
        if (firstAcquirer != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("firstAcquirer=");
            b.append(firstAcquirer.getValueString());
        }
        if (deliveryCount != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("deliveryCount=");
            b.append(deliveryCount.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Header " + getDisplayString() + "]";
    }
}
