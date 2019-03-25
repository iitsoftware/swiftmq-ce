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

import com.swiftmq.amqp.v100.generated.transport.definitions.SequenceNo;
import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * </p><p>
 * The properties section is used for a defined set of standard properties of the message.
 * The properties section is part of the bare message; therefore, if retransmitted by an
 * intermediary, it MUST remain unaltered.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Properties extends AMQPList
        implements SectionIF {
    public static String DESCRIPTOR_NAME = "amqp:properties:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000073L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    boolean dirty = false;

    MessageIdIF messageId = null;
    AMQPBinary userId = null;
    AddressIF to = null;
    AMQPString subject = null;
    AddressIF replyTo = null;
    MessageIdIF correlationId = null;
    AMQPSymbol contentType = null;
    AMQPSymbol contentEncoding = null;
    AMQPTimestamp absoluteExpiryTime = null;
    AMQPTimestamp creationTime = null;
    AMQPString groupId = null;
    SequenceNo groupSequence = null;
    AMQPString replyToGroupId = null;

    /**
     * Constructs a Properties.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Properties(List initValue) throws Exception {
        super(initValue);
        if (initValue != null)
            decode();
    }

    /**
     * Constructs a Properties.
     */
    public Properties() {
        dirty = true;
    }

    /**
     * Return whether this Properties has a descriptor
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
     * Returns the optional MessageId field.
     *
     * @return MessageId
     */
    public MessageIdIF getMessageId() {
        return messageId;
    }

    /**
     * Sets the optional MessageId field.
     *
     * @param messageId MessageId
     */
    public void setMessageId(MessageIdIF messageId) {
        dirty = true;
        this.messageId = messageId;
    }

    /**
     * Returns the optional UserId field.
     *
     * @return UserId
     */
    public AMQPBinary getUserId() {
        return userId;
    }

    /**
     * Sets the optional UserId field.
     *
     * @param userId UserId
     */
    public void setUserId(AMQPBinary userId) {
        dirty = true;
        this.userId = userId;
    }

    /**
     * Returns the optional To field.
     *
     * @return To
     */
    public AddressIF getTo() {
        return to;
    }

    /**
     * Sets the optional To field.
     *
     * @param to To
     */
    public void setTo(AddressIF to) {
        dirty = true;
        this.to = to;
    }

    /**
     * Returns the optional Subject field.
     *
     * @return Subject
     */
    public AMQPString getSubject() {
        return subject;
    }

    /**
     * Sets the optional Subject field.
     *
     * @param subject Subject
     */
    public void setSubject(AMQPString subject) {
        dirty = true;
        this.subject = subject;
    }

    /**
     * Returns the optional ReplyTo field.
     *
     * @return ReplyTo
     */
    public AddressIF getReplyTo() {
        return replyTo;
    }

    /**
     * Sets the optional ReplyTo field.
     *
     * @param replyTo ReplyTo
     */
    public void setReplyTo(AddressIF replyTo) {
        dirty = true;
        this.replyTo = replyTo;
    }

    /**
     * Returns the optional CorrelationId field.
     *
     * @return CorrelationId
     */
    public MessageIdIF getCorrelationId() {
        return correlationId;
    }

    /**
     * Sets the optional CorrelationId field.
     *
     * @param correlationId CorrelationId
     */
    public void setCorrelationId(MessageIdIF correlationId) {
        dirty = true;
        this.correlationId = correlationId;
    }

    /**
     * Returns the optional ContentType field.
     *
     * @return ContentType
     */
    public AMQPSymbol getContentType() {
        return contentType;
    }

    /**
     * Sets the optional ContentType field.
     *
     * @param contentType ContentType
     */
    public void setContentType(AMQPSymbol contentType) {
        dirty = true;
        this.contentType = contentType;
    }

    /**
     * Returns the optional ContentEncoding field.
     *
     * @return ContentEncoding
     */
    public AMQPSymbol getContentEncoding() {
        return contentEncoding;
    }

    /**
     * Sets the optional ContentEncoding field.
     *
     * @param contentEncoding ContentEncoding
     */
    public void setContentEncoding(AMQPSymbol contentEncoding) {
        dirty = true;
        this.contentEncoding = contentEncoding;
    }

    /**
     * Returns the optional AbsoluteExpiryTime field.
     *
     * @return AbsoluteExpiryTime
     */
    public AMQPTimestamp getAbsoluteExpiryTime() {
        return absoluteExpiryTime;
    }

    /**
     * Sets the optional AbsoluteExpiryTime field.
     *
     * @param absoluteExpiryTime AbsoluteExpiryTime
     */
    public void setAbsoluteExpiryTime(AMQPTimestamp absoluteExpiryTime) {
        dirty = true;
        this.absoluteExpiryTime = absoluteExpiryTime;
    }

    /**
     * Returns the optional CreationTime field.
     *
     * @return CreationTime
     */
    public AMQPTimestamp getCreationTime() {
        return creationTime;
    }

    /**
     * Sets the optional CreationTime field.
     *
     * @param creationTime CreationTime
     */
    public void setCreationTime(AMQPTimestamp creationTime) {
        dirty = true;
        this.creationTime = creationTime;
    }

    /**
     * Returns the optional GroupId field.
     *
     * @return GroupId
     */
    public AMQPString getGroupId() {
        return groupId;
    }

    /**
     * Sets the optional GroupId field.
     *
     * @param groupId GroupId
     */
    public void setGroupId(AMQPString groupId) {
        dirty = true;
        this.groupId = groupId;
    }

    /**
     * Returns the optional GroupSequence field.
     *
     * @return GroupSequence
     */
    public SequenceNo getGroupSequence() {
        return groupSequence;
    }

    /**
     * Sets the optional GroupSequence field.
     *
     * @param groupSequence GroupSequence
     */
    public void setGroupSequence(SequenceNo groupSequence) {
        dirty = true;
        this.groupSequence = groupSequence;
    }

    /**
     * Returns the optional ReplyToGroupId field.
     *
     * @return ReplyToGroupId
     */
    public AMQPString getReplyToGroupId() {
        return replyToGroupId;
    }

    /**
     * Sets the optional ReplyToGroupId field.
     *
     * @param replyToGroupId ReplyToGroupId
     */
    public void setReplyToGroupId(AMQPString replyToGroupId) {
        dirty = true;
        this.replyToGroupId = replyToGroupId;
    }

    /**
     * Returns the predicted size of this Properties. The predicted size may be greater than the actual size
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
            n += (messageId != null ? messageId.getPredictedSize() : 1);
            n += (userId != null ? userId.getPredictedSize() : 1);
            n += (to != null ? to.getPredictedSize() : 1);
            n += (subject != null ? subject.getPredictedSize() : 1);
            n += (replyTo != null ? replyTo.getPredictedSize() : 1);
            n += (correlationId != null ? correlationId.getPredictedSize() : 1);
            n += (contentType != null ? contentType.getPredictedSize() : 1);
            n += (contentEncoding != null ? contentEncoding.getPredictedSize() : 1);
            n += (absoluteExpiryTime != null ? absoluteExpiryTime.getPredictedSize() : 1);
            n += (creationTime != null ? creationTime.getPredictedSize() : 1);
            n += (groupId != null ? groupId.getPredictedSize() : 1);
            n += (groupSequence != null ? groupSequence.getPredictedSize() : 1);
            n += (replyToGroupId != null ? replyToGroupId.getPredictedSize() : 1);
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

        // Field: messageId
        // Type     : MessageIdIF, converted: MessageIdIF
        // Basetype : MessageIdIF
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : MessageIdFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() != AMQPTypeDecoder.NULL)
            messageId = MessageIdFactory.create(t);

        // Field: userId
        // Type     : binary, converted: AMQPBinary
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
                userId = (AMQPBinary) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'userId' in 'Properties' type: " + e);
        }

        // Field: to
        // Type     : AddressIF, converted: AddressIF
        // Basetype : AddressIF
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : AddressFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() != AMQPTypeDecoder.NULL)
            to = AddressFactory.create(t);

        // Field: subject
        // Type     : string, converted: AMQPString
        // Basetype : string
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                subject = (AMQPString) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'subject' in 'Properties' type: " + e);
        }

        // Field: replyTo
        // Type     : AddressIF, converted: AddressIF
        // Basetype : AddressIF
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : AddressFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() != AMQPTypeDecoder.NULL)
            replyTo = AddressFactory.create(t);

        // Field: correlationId
        // Type     : MessageIdIF, converted: MessageIdIF
        // Basetype : MessageIdIF
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : MessageIdFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() != AMQPTypeDecoder.NULL)
            correlationId = MessageIdFactory.create(t);

        // Field: contentType
        // Type     : symbol, converted: AMQPSymbol
        // Basetype : symbol
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                contentType = (AMQPSymbol) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'contentType' in 'Properties' type: " + e);
        }

        // Field: contentEncoding
        // Type     : symbol, converted: AMQPSymbol
        // Basetype : symbol
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                contentEncoding = (AMQPSymbol) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'contentEncoding' in 'Properties' type: " + e);
        }

        // Field: absoluteExpiryTime
        // Type     : timestamp, converted: AMQPTimestamp
        // Basetype : timestamp
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                absoluteExpiryTime = (AMQPTimestamp) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'absoluteExpiryTime' in 'Properties' type: " + e);
        }

        // Field: creationTime
        // Type     : timestamp, converted: AMQPTimestamp
        // Basetype : timestamp
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                creationTime = (AMQPTimestamp) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'creationTime' in 'Properties' type: " + e);
        }

        // Field: groupId
        // Type     : string, converted: AMQPString
        // Basetype : string
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                groupId = (AMQPString) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'groupId' in 'Properties' type: " + e);
        }

        // Field: groupSequence
        // Type     : SequenceNo, converted: SequenceNo
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
                groupSequence = new SequenceNo(((AMQPUnsignedInt) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'groupSequence' in 'Properties' type: " + e);
        }

        // Field: replyToGroupId
        // Type     : string, converted: AMQPString
        // Basetype : string
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                replyToGroupId = (AMQPString) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'replyToGroupId' in 'Properties' type: " + e);
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
        addToList(l, messageId);
        addToList(l, userId);
        addToList(l, to);
        addToList(l, subject);
        addToList(l, replyTo);
        addToList(l, correlationId);
        addToList(l, contentType);
        addToList(l, contentEncoding);
        addToList(l, absoluteExpiryTime);
        addToList(l, creationTime);
        addToList(l, groupId);
        addToList(l, groupSequence);
        addToList(l, replyToGroupId);
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
        StringBuffer b = new StringBuffer("[Properties ");
        b.append(getDisplayString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (messageId != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("messageId=");
            b.append(messageId.getValueString());
        }
        if (userId != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("userId=");
            b.append(userId.getValueString());
        }
        if (to != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("to=");
            b.append(to.getValueString());
        }
        if (subject != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("subject=");
            b.append(subject.getValueString());
        }
        if (replyTo != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("replyTo=");
            b.append(replyTo.getValueString());
        }
        if (correlationId != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("correlationId=");
            b.append(correlationId.getValueString());
        }
        if (contentType != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("contentType=");
            b.append(contentType.getValueString());
        }
        if (contentEncoding != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("contentEncoding=");
            b.append(contentEncoding.getValueString());
        }
        if (absoluteExpiryTime != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("absoluteExpiryTime=");
            b.append(absoluteExpiryTime.getValueString());
        }
        if (creationTime != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("creationTime=");
            b.append(creationTime.getValueString());
        }
        if (groupId != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("groupId=");
            b.append(groupId.getValueString());
        }
        if (groupSequence != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("groupSequence=");
            b.append(groupSequence.getValueString());
        }
        if (replyToGroupId != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("replyToGroupId=");
            b.append(replyToGroupId.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Properties " + getDisplayString() + "]";
    }
}
