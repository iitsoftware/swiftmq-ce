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

package com.swiftmq.amqp.v100.generated.transport.performatives;

import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateFactory;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.DeliveryNumber;
import com.swiftmq.amqp.v100.generated.transport.definitions.Role;
import com.swiftmq.amqp.v100.transport.AMQPFrame;
import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * </p><p>
 * The disposition frame is used to inform the remote peer of local changes in the state of
 * deliveries. The disposition frame may reference deliveries from many different links
 * associated with a session, although all links MUST have the directionality indicated by
 * the specified  role .
 * </p><p>
 * </p><p>
 * Note that it is possible for a disposition sent from sender to receiver to refer to a
 * delivery which has not yet completed (i.e., a delivery which is spread over multiple
 * frames and not all frames have yet been sent). The use of such interleaving is
 * discouraged in favor of carrying the modified state on the next
 * performative for the delivery.
 * </p><p>
 * </p><p>
 * The disposition performative may refer to deliveries on links that are no longer attached.
 * As long as the links have not been closed or detached with an error then the deliveries
 * are still "live" and the updated state MUST be applied.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class DispositionFrame extends AMQPFrame
        implements FrameIF {
    public static String DESCRIPTOR_NAME = "amqp:disposition:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000015L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    AMQPList body = null;
    boolean dirty = false;

    Role role = null;
    DeliveryNumber first = null;
    DeliveryNumber last = null;
    AMQPBoolean settled = AMQPBoolean.FALSE;
    DeliveryStateIF state = null;
    AMQPBoolean batchable = AMQPBoolean.FALSE;

    /**
     * Constructs a DispositionFrame.
     *
     * @param channel the channel id
     * @param body    the frame body
     */
    public DispositionFrame(int channel, AMQPList body) throws Exception {
        super(channel);
        this.body = body;
        if (body != null)
            decode();
    }

    /**
     * Constructs a DispositionFrame.
     *
     * @param channel the channel id
     */
    public DispositionFrame(int channel) {
        super(channel);
    }

    /**
     * Accept method for a Frame visitor.
     *
     * @param visitor Frame visitor
     */
    public void accept(FrameVisitor visitor) {
        visitor.visit(this);
    }


    /**
     * Sets the mandatory Role field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * The role identifies whether the disposition frame contains information
     * about  sending  link endpoints or  receiving  link endpoints.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param role Role
     */
    public void setRole(Role role) {
        dirty = true;
        this.role = role;
    }

    /**
     * Returns the mandatory Role field.
     *
     * @return Role
     */
    public Role getRole() {
        return role;
    }

    /**
     * Sets the mandatory First field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * Identifies the lower bound of delivery-ids for the deliveries in this set.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param first First
     */
    public void setFirst(DeliveryNumber first) {
        dirty = true;
        this.first = first;
    }

    /**
     * Returns the mandatory First field.
     *
     * @return First
     */
    public DeliveryNumber getFirst() {
        return first;
    }

    /**
     * Sets the optional Last field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * Identifies the upper bound of delivery-ids for the deliveries in this set. If not set,
     * this is taken to be the same as  first .
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param last Last
     */
    public void setLast(DeliveryNumber last) {
        dirty = true;
        this.last = last;
    }

    /**
     * Returns the optional Last field.
     *
     * @return Last
     */
    public DeliveryNumber getLast() {
        return last;
    }

    /**
     * Sets the optional Settled field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * If true, indicates that the referenced deliveries are considered settled by the issuing
     * endpoint.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param settled Settled
     */
    public void setSettled(AMQPBoolean settled) {
        dirty = true;
        this.settled = settled;
    }

    /**
     * Returns the optional Settled field.
     *
     * @return Settled
     */
    public AMQPBoolean getSettled() {
        return settled;
    }

    /**
     * Sets the optional State field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * Communicates the state of all the deliveries referenced by this disposition.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param state State
     */
    public void setState(DeliveryStateIF state) {
        dirty = true;
        this.state = state;
    }

    /**
     * Returns the optional State field.
     *
     * @return State
     */
    public DeliveryStateIF getState() {
        return state;
    }

    /**
     * Sets the optional Batchable field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * If true, then the issuer is hinting that there is no need for the peer to urgently
     * communicate the impact of the updated delivery states. This hint may be used to
     * artificially increase the amount of batching an implementation uses when communicating
     * delivery states, and thereby save bandwidth.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param batchable Batchable
     */
    public void setBatchable(AMQPBoolean batchable) {
        dirty = true;
        this.batchable = batchable;
    }

    /**
     * Returns the optional Batchable field.
     *
     * @return Batchable
     */
    public AMQPBoolean getBatchable() {
        return batchable;
    }


    /**
     * Returns the predicted size of this DispositionFrame. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        int n = super.getPredictedSize();
        if (body == null) {
            n += 4; // For safety (length field and size of the list)
            n += codeConstructor.getPredictedSize();
            n += (role != null ? role.getPredictedSize() : 1);
            n += (first != null ? first.getPredictedSize() : 1);
            n += (last != null ? last.getPredictedSize() : 1);
            n += (settled != null ? settled.getPredictedSize() : 1);
            n += (state != null ? state.getPredictedSize() : 1);
            n += (batchable != null ? batchable.getPredictedSize() : 1);
        } else
            n += body.getPredictedSize();
        return n;
    }

    private AMQPArray singleToArray(AMQPType t) throws IOException {
        return new AMQPArray(t.getCode(), new AMQPType[]{t});
    }

    private void decode() throws Exception {
        List l = body.getValue();
        AMQPType t = null;
        int idx = 0;


        // Field: role
        // Type     : Role, converted: Role
        // Basetype : boolean
        // Default  : null
        // Mandatory: true
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() == AMQPTypeDecoder.NULL)
            throw new Exception("Mandatory field 'role' in 'Disposition' frame is NULL");
        try {
            role = new Role(((AMQPBoolean) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'role' in 'Disposition' frame: " + e);
        }


        // Field: first
        // Type     : DeliveryNumber, converted: DeliveryNumber
        // Basetype : uint
        // Default  : null
        // Mandatory: true
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() == AMQPTypeDecoder.NULL)
            throw new Exception("Mandatory field 'first' in 'Disposition' frame is NULL");
        try {
            first = new DeliveryNumber(((AMQPUnsignedInt) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'first' in 'Disposition' frame: " + e);
        }


        // Field: last
        // Type     : DeliveryNumber, converted: DeliveryNumber
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
                last = new DeliveryNumber(((AMQPUnsignedInt) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'last' in 'Disposition' frame: " + e);
        }


        // Field: settled
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
                settled = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'settled' in 'Disposition' frame: " + e);
        }


        // Field: state
        // Type     : DeliveryStateIF, converted: DeliveryStateIF
        // Basetype : DeliveryStateIF
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : DeliveryStateFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() != AMQPTypeDecoder.NULL)
            state = DeliveryStateFactory.create(t);


        // Field: batchable
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
                batchable = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'batchable' in 'Disposition' frame: " + e);
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
        addToList(l, role);
        addToList(l, first);
        addToList(l, last);
        addToList(l, settled);
        addToList(l, state);
        addToList(l, batchable);
        for (ListIterator iter = l.listIterator(l.size()); iter.hasPrevious(); ) {
            AMQPType t = (AMQPType) iter.previous();
            if (t.getCode() == AMQPTypeDecoder.NULL)
                iter.remove();
            else
                break;
        }
        body = new AMQPList(l);
        dirty = false;
    }

    protected void writeBody(DataOutput out) throws IOException {
        if (dirty || body == null)
            encode();
        codeConstructor.setFormatCode(body.getCode());
        body.setConstructor(codeConstructor);
        body.writeContent(out);
    }

    /**
     * Returns a value representation of this DispositionFrame.
     *
     * @return value representation
     */
    public String getValueString() {
        try {
            if (dirty || body == null)
                encode();
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuffer b = new StringBuffer("[Disposition ");
        b.append(super.getValueString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (role != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("role=");
            b.append(role.getValueString());
        }
        if (first != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("first=");
            b.append(first.getValueString());
        }
        if (last != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("last=");
            b.append(last.getValueString());
        }
        if (settled != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("settled=");
            b.append(settled.getValueString());
        }
        if (state != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("state=");
            b.append(state.getValueString());
        }
        if (batchable != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("batchable=");
            b.append(batchable.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Disposition " + getDisplayString() + "]";
    }
}
