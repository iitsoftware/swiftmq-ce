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
import com.swiftmq.amqp.v100.generated.transport.definitions.*;
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
 * The transfer frame is used to send messages across a link. Messages may be carried by a
 * single transfer up to the maximum negotiated frame size for the connection. Larger
 * messages may be split across several transfer frames.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class TransferFrame extends AMQPFrame
        implements FrameIF {
    public static String DESCRIPTOR_NAME = "amqp:transfer:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000014L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    AMQPList body = null;
    boolean dirty = false;

    Handle handle = null;
    DeliveryNumber deliveryId = null;
    DeliveryTag deliveryTag = null;
    MessageFormat messageFormat = null;
    AMQPBoolean settled = null;
    AMQPBoolean more = AMQPBoolean.FALSE;
    ReceiverSettleMode rcvSettleMode = null;
    DeliveryStateIF state = null;
    AMQPBoolean resume = AMQPBoolean.FALSE;
    AMQPBoolean aborted = AMQPBoolean.FALSE;
    AMQPBoolean batchable = AMQPBoolean.FALSE;

    /**
     * Constructs a TransferFrame.
     *
     * @param channel the channel id
     * @param body    the frame body
     */
    public TransferFrame(int channel, AMQPList body) throws Exception {
        super(channel);
        this.body = body;
        if (body != null)
            decode();
    }

    /**
     * Constructs a TransferFrame.
     *
     * @param channel the channel id
     */
    public TransferFrame(int channel) {
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
     * Sets the mandatory Handle field.
     *
     * <p>
     * </p><p>
     * Specifies the link on which the message is transferred.
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param handle Handle
     */
    public void setHandle(Handle handle) {
        dirty = true;
        this.handle = handle;
    }

    /**
     * Returns the mandatory Handle field.
     *
     * @return Handle
     */
    public Handle getHandle() {
        return handle;
    }

    /**
     * Sets the optional DeliveryId field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * The delivery-id MUST be supplied on the first transfer of a multi-transfer delivery. On
     * continuation transfers the delivery-id MAY be omitted. It is an error if the delivery-id
     * on a continuation transfer differs from the delivery-id on the first transfer of a
     * delivery.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param deliveryId DeliveryId
     */
    public void setDeliveryId(DeliveryNumber deliveryId) {
        dirty = true;
        this.deliveryId = deliveryId;
    }

    /**
     * Returns the optional DeliveryId field.
     *
     * @return DeliveryId
     */
    public DeliveryNumber getDeliveryId() {
        return deliveryId;
    }

    /**
     * Sets the optional DeliveryTag field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * Uniquely identifies the delivery attempt for a given message on this link. This field
     * MUST be specified for the first transfer of a multi-transfer message and may only be
     * omitted for continuation transfers. It is an error if the delivery-tag on a continuation
     * transfer differs from the delivery-tag on the first transfer of a delivery.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param deliveryTag DeliveryTag
     */
    public void setDeliveryTag(DeliveryTag deliveryTag) {
        dirty = true;
        this.deliveryTag = deliveryTag;
    }

    /**
     * Returns the optional DeliveryTag field.
     *
     * @return DeliveryTag
     */
    public DeliveryTag getDeliveryTag() {
        return deliveryTag;
    }

    /**
     * Sets the optional MessageFormat field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * This field MUST be specified for the first transfer of a multi-transfer message and may
     * only be omitted for continuation transfers. It is an error if the message-format on a
     * continuation transfer differs from the message-format on the first transfer of a
     * delivery.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param messageFormat MessageFormat
     */
    public void setMessageFormat(MessageFormat messageFormat) {
        dirty = true;
        this.messageFormat = messageFormat;
    }

    /**
     * Returns the optional MessageFormat field.
     *
     * @return MessageFormat
     */
    public MessageFormat getMessageFormat() {
        return messageFormat;
    }

    /**
     * Sets the optional Settled field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * If not set on the first (or only) transfer for a (multi-transfer) delivery, then the
     * settled flag MUST be interpreted as being false. For subsequent transfers in a
     * multi-transfer delivery if the settled flag is left unset then it MUST be interpreted
     * as true if and only if the value of the settled flag on any of the preceding transfers
     * was true; if no preceding transfer was sent with settled being true then the value when
     * unset MUST be taken as false.
     * </p><p>
     * </p><p>
     * If the negotiated value for snd-settle-mode at attachment is  , then this field MUST be true on at least
     * one transfer frame for a delivery (i.e., the delivery must be settled at the sender at
     * the point the delivery has been completely transferred).
     * </p><p>
     * </p><p>
     * If the negotiated value for snd-settle-mode at attachment is  , then this field MUST be false (or
     * unset) on every transfer frame for a delivery (unless the delivery is aborted).
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
     * Sets the optional More field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * Note that if both the more and aborted fields are set to true, the aborted flag takes
     * precedence. That is, a receiver should ignore the value of the more field if the
     * transfer is marked as aborted. A sender SHOULD NOT set the more flag to true if it
     * also sets the aborted flag to true.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param more More
     */
    public void setMore(AMQPBoolean more) {
        dirty = true;
        this.more = more;
    }

    /**
     * Returns the optional More field.
     *
     * @return More
     */
    public AMQPBoolean getMore() {
        return more;
    }

    /**
     * Sets the optional RcvSettleMode field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * If  , this indicates that the
     * receiver MUST settle the delivery once it has arrived without waiting for the sender to
     * settle first.
     * </p><p>
     * </p><p>
     * If  , this indicates that the
     * receiver MUST NOT settle until sending its disposition to the sender and receiving a
     * settled disposition from the sender.
     * </p><p>
     * </p><p>
     * If not set, this value is defaulted to the value negotiated on link attach.
     * </p><p>
     * </p><p>
     * If the negotiated link value is  ,
     * then it is illegal to set this field to  .
     * </p><p>
     * </p><p>
     * If the message is being sent settled by the sender, the value of this field is ignored.
     * </p><p>
     * </p><p>
     * The (implicit or explicit) value of this field does not form part of the transfer state,
     * and is not retained if a link is suspended and subsequently resumed.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param rcvSettleMode RcvSettleMode
     */
    public void setRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        dirty = true;
        this.rcvSettleMode = rcvSettleMode;
    }

    /**
     * Returns the optional RcvSettleMode field.
     *
     * @return RcvSettleMode
     */
    public ReceiverSettleMode getRcvSettleMode() {
        return rcvSettleMode;
    }

    /**
     * Sets the optional State field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * When set this informs the receiver of the state of the delivery at the sender. This is
     * particularly useful when transfers of unsettled deliveries are resumed after resuming a
     * link. Setting the state on the transfer can be thought of as being equivalent to sending
     * a disposition immediately before the   performative, i.e., it is
     * the state of the delivery (not the transfer) that existed at the point the frame was
     * sent.
     * </p><p>
     * </p><p>
     * Note that if the   performative (or an earlier   performative referring to the delivery) indicates that the delivery
     * has attained a terminal state, then no future   or   sent by the sender can alter that terminal state.
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
     * Sets the optional Resume field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * If true, the resume flag indicates that the transfer is being used to reassociate an
     * unsettled delivery from a dissociated link endpoint. See
     * for more details.
     * </p><p>
     * </p><p>
     * The receiver MUST ignore resumed deliveries that are not in its local unsettled map. The
     * sender MUST NOT send resumed transfers for deliveries not in its local unsettled map.
     * </p><p>
     * </p><p>
     * If a resumed delivery spans more than one transfer performative, then the resume flag
     * MUST be set to true on the first transfer of the resumed delivery. For subsequent
     * transfers for the same delivery the resume flag may be set to true, or may be omitted.
     * </p><p>
     * </p><p>
     * In the case where the exchange of unsettled maps makes clear that all message data has
     * been successfully transferred to the receiver, and that only the final state (and
     * potentially settlement) at the sender needs to be conveyed, then a resumed delivery may
     * carry no payload and instead act solely as a vehicle for carrying the terminal state of
     * the delivery at the sender.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param resume Resume
     */
    public void setResume(AMQPBoolean resume) {
        dirty = true;
        this.resume = resume;
    }

    /**
     * Returns the optional Resume field.
     *
     * @return Resume
     */
    public AMQPBoolean getResume() {
        return resume;
    }

    /**
     * Sets the optional Aborted field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * Aborted messages should be discarded by the recipient (any payload within the frame
     * carrying the performative MUST be ignored). An aborted message is implicitly settled.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param aborted Aborted
     */
    public void setAborted(AMQPBoolean aborted) {
        dirty = true;
        this.aborted = aborted;
    }

    /**
     * Returns the optional Aborted field.
     *
     * @return Aborted
     */
    public AMQPBoolean getAborted() {
        return aborted;
    }

    /**
     * Sets the optional Batchable field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * If true, then the issuer is hinting that there is no need for the peer to urgently
     * communicate updated delivery state. This hint may be used to artificially increase the
     * amount of batching an implementation uses when communicating delivery states, and
     * thereby save bandwidth.
     * </p><p>
     * </p><p>
     * If the message being delivered is too large to fit within a single frame, then the
     * setting of batchable to true on any of the   performatives for the
     * delivery is equivalent to setting batchable to true for all the
     * performatives for the delivery.
     * </p><p>
     * </p><p>
     * The batchable value does not form part of the transfer state, and is not retained if
     * a link is suspended and subsequently resumed.
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
     * Returns the predicted size of this TransferFrame. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        int n = super.getPredictedSize();
        if (body == null) {
            n += 4; // For safety (length field and size of the list)
            n += codeConstructor.getPredictedSize();
            n += (handle != null ? handle.getPredictedSize() : 1);
            n += (deliveryId != null ? deliveryId.getPredictedSize() : 1);
            n += (deliveryTag != null ? deliveryTag.getPredictedSize() : 1);
            n += (messageFormat != null ? messageFormat.getPredictedSize() : 1);
            n += (settled != null ? settled.getPredictedSize() : 1);
            n += (more != null ? more.getPredictedSize() : 1);
            n += (rcvSettleMode != null ? rcvSettleMode.getPredictedSize() : 1);
            n += (state != null ? state.getPredictedSize() : 1);
            n += (resume != null ? resume.getPredictedSize() : 1);
            n += (aborted != null ? aborted.getPredictedSize() : 1);
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


        // Field: handle
        // Type     : Handle, converted: Handle
        // Basetype : uint
        // Default  : null
        // Mandatory: true
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() == AMQPTypeDecoder.NULL)
            throw new Exception("Mandatory field 'handle' in 'Transfer' frame is NULL");
        try {
            handle = new Handle(((AMQPUnsignedInt) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'handle' in 'Transfer' frame: " + e);
        }


        // Field: deliveryId
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
                deliveryId = new DeliveryNumber(((AMQPUnsignedInt) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'deliveryId' in 'Transfer' frame: " + e);
        }


        // Field: deliveryTag
        // Type     : DeliveryTag, converted: DeliveryTag
        // Basetype : binary
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                deliveryTag = new DeliveryTag(((AMQPBinary) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'deliveryTag' in 'Transfer' frame: " + e);
        }


        // Field: messageFormat
        // Type     : MessageFormat, converted: MessageFormat
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
                messageFormat = new MessageFormat(((AMQPUnsignedInt) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'messageFormat' in 'Transfer' frame: " + e);
        }


        // Field: settled
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
                settled = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'settled' in 'Transfer' frame: " + e);
        }


        // Field: more
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
                more = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'more' in 'Transfer' frame: " + e);
        }


        // Field: rcvSettleMode
        // Type     : ReceiverSettleMode, converted: ReceiverSettleMode
        // Basetype : ubyte
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                rcvSettleMode = new ReceiverSettleMode(((AMQPUnsignedByte) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'rcvSettleMode' in 'Transfer' frame: " + e);
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


        // Field: resume
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
                resume = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'resume' in 'Transfer' frame: " + e);
        }


        // Field: aborted
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
                aborted = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'aborted' in 'Transfer' frame: " + e);
        }


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
            throw new Exception("Invalid type of field 'batchable' in 'Transfer' frame: " + e);
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
        addToList(l, handle);
        addToList(l, deliveryId);
        addToList(l, deliveryTag);
        addToList(l, messageFormat);
        addToList(l, settled);
        addToList(l, more);
        addToList(l, rcvSettleMode);
        addToList(l, state);
        addToList(l, resume);
        addToList(l, aborted);
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
     * Returns a value representation of this TransferFrame.
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
        StringBuffer b = new StringBuffer("[Transfer ");
        b.append(super.getValueString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (handle != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("handle=");
            b.append(handle.getValueString());
        }
        if (deliveryId != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("deliveryId=");
            b.append(deliveryId.getValueString());
        }
        if (deliveryTag != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("deliveryTag=");
            b.append(deliveryTag.getValueString());
        }
        if (messageFormat != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("messageFormat=");
            b.append(messageFormat.getValueString());
        }
        if (settled != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("settled=");
            b.append(settled.getValueString());
        }
        if (more != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("more=");
            b.append(more.getValueString());
        }
        if (rcvSettleMode != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("rcvSettleMode=");
            b.append(rcvSettleMode.getValueString());
        }
        if (state != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("state=");
            b.append(state.getValueString());
        }
        if (resume != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("resume=");
            b.append(resume.getValueString());
        }
        if (aborted != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("aborted=");
            b.append(aborted.getValueString());
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
        return "[Transfer " + getDisplayString() + ", body=" + body.getValueString() + "]";
    }
}
