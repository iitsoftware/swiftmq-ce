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

import com.swiftmq.amqp.v100.generated.transport.definitions.Fields;
import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * </p><p>
 * At the source the modified outcome means that the message is no longer acquired by the
 * receiver, and has been made available for (re-)delivery to the same or other targets
 * receiving from the node. The message has been changed at the node in the ways indicated by
 * the fields of the outcome. As modified is a terminal outcome, transfer of payload data
 * will not be able to be resumed if the link becomes suspended. A delivery may become
 * modified at the source even before all transfer frames have been sent. This does not imply
 * that the remaining transfers for the delivery will not be sent. The source MAY
 * spontaneously attain the modified outcome for a message (for example the source may
 * implement some sort of time-bound acquisition lock, after which the acquisition of a
 * message at a node is revoked to allow for delivery to an alternative consumer with the
 * message modified in some way to denote the previous failed, e.g., with delivery-failed
 * set to true).
 * </p><p>
 * </p><p>
 * At the target, the modified outcome is used to indicate that a given transfer was not and
 * will not be acted upon, and that the message should be modified in the specified ways at
 * the node.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Modified extends AMQPList
        implements DeliveryStateIF, OutcomeIF {
    public static String DESCRIPTOR_NAME = "amqp:modified:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000027L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    boolean dirty = false;

    AMQPBoolean deliveryFailed = null;
    AMQPBoolean undeliverableHere = null;
    Fields messageAnnotations = null;

    /**
     * Constructs a Modified.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Modified(List initValue) throws Exception {
        super(initValue);
        if (initValue != null)
            decode();
    }

    /**
     * Constructs a Modified.
     */
    public Modified() {
        dirty = true;
    }

    /**
     * Return whether this Modified has a descriptor
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
     * Returns the optional DeliveryFailed field.
     *
     * @return DeliveryFailed
     */
    public AMQPBoolean getDeliveryFailed() {
        return deliveryFailed;
    }

    /**
     * Sets the optional DeliveryFailed field.
     *
     * @param deliveryFailed DeliveryFailed
     */
    public void setDeliveryFailed(AMQPBoolean deliveryFailed) {
        dirty = true;
        this.deliveryFailed = deliveryFailed;
    }

    /**
     * Returns the optional UndeliverableHere field.
     *
     * @return UndeliverableHere
     */
    public AMQPBoolean getUndeliverableHere() {
        return undeliverableHere;
    }

    /**
     * Sets the optional UndeliverableHere field.
     *
     * @param undeliverableHere UndeliverableHere
     */
    public void setUndeliverableHere(AMQPBoolean undeliverableHere) {
        dirty = true;
        this.undeliverableHere = undeliverableHere;
    }

    /**
     * Returns the optional MessageAnnotations field.
     *
     * @return MessageAnnotations
     */
    public Fields getMessageAnnotations() {
        return messageAnnotations;
    }

    /**
     * Sets the optional MessageAnnotations field.
     *
     * @param messageAnnotations MessageAnnotations
     */
    public void setMessageAnnotations(Fields messageAnnotations) {
        dirty = true;
        this.messageAnnotations = messageAnnotations;
    }

    /**
     * Returns the predicted size of this Modified. The predicted size may be greater than the actual size
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
            n += (deliveryFailed != null ? deliveryFailed.getPredictedSize() : 1);
            n += (undeliverableHere != null ? undeliverableHere.getPredictedSize() : 1);
            n += (messageAnnotations != null ? messageAnnotations.getPredictedSize() : 1);
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

        // Field: deliveryFailed
        // Type     : boolean, converted: AMQPBoolean
        // Basetype : boolean
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                deliveryFailed = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'deliveryFailed' in 'Modified' type: " + e);
        }

        // Field: undeliverableHere
        // Type     : boolean, converted: AMQPBoolean
        // Basetype : boolean
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                undeliverableHere = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'undeliverableHere' in 'Modified' type: " + e);
        }

        // Field: messageAnnotations
        // Type     : Fields, converted: Fields
        // Basetype : map
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                messageAnnotations = new Fields(((AMQPMap) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'messageAnnotations' in 'Modified' type: " + e);
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
        addToList(l, deliveryFailed);
        addToList(l, undeliverableHere);
        addToList(l, messageAnnotations);
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
        StringBuffer b = new StringBuffer("[Modified ");
        b.append(getDisplayString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (deliveryFailed != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("deliveryFailed=");
            b.append(deliveryFailed.getValueString());
        }
        if (undeliverableHere != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("undeliverableHere=");
            b.append(undeliverableHere.getValueString());
        }
        if (messageAnnotations != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("messageAnnotations=");
            b.append(messageAnnotations.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Modified " + getDisplayString() + "]";
    }
}
