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

package com.swiftmq.amqp.v100.messaging;

import com.swiftmq.amqp.v100.client.Consumer;
import com.swiftmq.amqp.v100.client.InvalidStateException;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.Accepted;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.Rejected;
import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TransactionalState;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.DeliveryTag;
import com.swiftmq.amqp.v100.types.AMQPType;
import com.swiftmq.amqp.v100.types.AMQPTypeDecoder;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataStreamInputStream;
import com.swiftmq.tools.util.MultiByteArrayInputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * AMQP message implementation.
 * </p>
 * <p>
 * The message may have different sections: Header, DeliveryAnnotations, MessageAnnotations, Properties, ApplicationProperties, a body and a Footer.
 * The body is either one or more data sections, one or more AmqpSequence sections, or an AmqpValue section.
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPMessage {
    Header header = null;
    DeliveryAnnotations deliveryAnnotations = null;
    MessageAnnotations messageAnnotations = null;
    Properties properties = null;
    ApplicationProperties applicationProperties = null;
    List<Data> data = null;
    List<AmqpSequence> amqpSequence = null;
    AmqpValue amqpValue = null;
    Footer footer = null;

    BodyVisitor visitor = new BodyVisitor();
    Exception parseException = null;

    byte[] body = null;
    byte[][] multiBody = null;
    int totalSize = 0;
    int bodySize = 0;

    DeliveryTag deliveryTag = null;
    long deliveryId = -1;
    boolean settled = false;

    TxnIdIF txnIdIF = null;
    Consumer consumer = null;

    /**
     * Create an AMQP message which decodes itself out of a binary array.
     *
     * @param body binary array
     * @throws Exception on error
     */
    public AMQPMessage(byte[] body) throws Exception {
        this.body = body;
        decode();
    }

    /**
     * Create an AMQP message which decodes itself out of multiple binary arrays.
     *
     * @param multiBody multiple binary arrays
     * @param totalSize the totalSize of the arrays
     * @throws Exception on error
     */
    public AMQPMessage(byte[][] multiBody, int totalSize) throws Exception {
        this.multiBody = multiBody;
        this.totalSize = totalSize;
        decode();
    }

    /**
     * Constructs an empty AMQP message.
     */
    public AMQPMessage() {
    }

    /**
     * Returns the size of the message
     *
     * @return size
     */
    public int getBodySize() {
        return bodySize;
    }

    /**
     * Sets the consumer which received this message. Internal use only.
     *
     * @param consumer Consumer
     */
    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    /**
     * Returns the transaction id.
     *
     * @return transaction id
     */
    public TxnIdIF getTxnIdIF() {
        return txnIdIF;
    }

    /**
     * Sets the transaction id.
     *
     * @param txnIdIF transaction id.
     */
    public void setTxnIdIF(TxnIdIF txnIdIF) {
        this.txnIdIF = txnIdIF;
    }

    /**
     * Accepts delivery of this message. This is part of settlement with QoS modes at-least-once and exactly-once.
     *
     * @throws InvalidStateException If the state is invalid to perform this operation
     */
    public void accept() throws InvalidStateException {
        if (consumer == null)
            throw new InvalidStateException("Message not associated with a consumer");
        if (settled && txnIdIF == null)
            throw new InvalidStateException("Accept not required; message has already been settled");
        DeliveryStateIF deliveryStateIF = null;
        if (txnIdIF == null)
            deliveryStateIF = new Accepted();
        else {
            TransactionalState transactionalState = new TransactionalState();
            transactionalState.setTxnId(txnIdIF);
            transactionalState.setOutcome(new Accepted());
            deliveryStateIF = transactionalState;
        }
        consumer.sendDisposition(this, deliveryStateIF);
    }

    /**
     * <p>Rejects delivery of this message. This is part of settlement with QoS modes at-least-once and exactly-once.
     * </p>
     * <p>
     * This operation causes the delivery count to be incremented and the message to be redelivered to this or other
     * competing consumers.
     * </p>
     *
     * @throws InvalidStateException If the state is invalid to perform this operation
     */
    public void reject() throws InvalidStateException {
        if (consumer == null)
            throw new InvalidStateException("Message not associated with a consumer");
        if (settled && txnIdIF == null)
            throw new InvalidStateException("Reject not possible; message has already been settled");
        DeliveryStateIF deliveryStateIF = null;
        if (txnIdIF == null)
            deliveryStateIF = new Rejected();
        else {
            TransactionalState transactionalState = new TransactionalState();
            transactionalState.setTxnId(txnIdIF);
            transactionalState.setOutcome(new Rejected());
            deliveryStateIF = transactionalState;
        }
        consumer.sendDisposition(this, deliveryStateIF);
    }

    /**
     * Return the Header section.
     *
     * @return Header section
     */
    public Header getHeader() {
        return header;
    }

    /**
     * Sets the Header section.
     *
     * @param header Header section
     */
    public void setHeader(Header header) {
        this.header = header;
        body = null;
    }

    /**
     * Returns the DeliveryAnnotations section.
     *
     * @return DeliveryAnnotations section
     */
    public DeliveryAnnotations getDeliveryAnnotations() {
        return deliveryAnnotations;
    }

    /**
     * Sets the DeliveryAnnotations section.
     *
     * @param deliveryAnnotations DeliveryAnnotations section
     */
    public void setDeliveryAnnotations(DeliveryAnnotations deliveryAnnotations) {
        this.deliveryAnnotations = deliveryAnnotations;
        body = null;
    }

    /**
     * Returns the MessageAnnotations section.
     *
     * @return MessageAnnotations section
     */
    public MessageAnnotations getMessageAnnotations() {
        return messageAnnotations;
    }

    /**
     * Sets the MessageAnnotations section.
     *
     * @param messageAnnotations MessageAnnotations section
     */
    public void setMessageAnnotations(MessageAnnotations messageAnnotations) {
        this.messageAnnotations = messageAnnotations;
        body = null;
    }

    /**
     * Returns the Properties section.
     *
     * @return Properties section
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the Properties section.
     *
     * @param properties Properties section
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
        body = null;
    }

    /**
     * Returns the ApplicationProperties section.
     *
     * @return ApplicationProperties section
     */
    public ApplicationProperties getApplicationProperties() {
        return applicationProperties;
    }

    /**
     * Sets the ApplicationProperties section.
     *
     * @param applicationProperties ApplicationProperties section
     */
    public void setApplicationProperties(ApplicationProperties applicationProperties) {
        this.applicationProperties = applicationProperties;
        body = null;
    }

    /**
     * Returns a list of Data body sections.
     *
     * @return list of Data body sections
     */
    public List<Data> getData() {
        return data;
    }

    /**
     * Add a Data body section.
     *
     * @param d Data body section
     */
    public void addData(Data d) {
        if (data == null)
            data = new ArrayList();
        data.add(d);
        body = null;
        amqpSequence = null;
        amqpValue = null;
    }

    /**
     * Returns a list of AmqpSequence body sections.
     *
     * @return list of AmqpSequence body sections
     */
    public List<AmqpSequence> getAmqpSequence() {
        return amqpSequence;
    }

    /**
     * Add a AmqpSequence body section.
     *
     * @param sequence AmqpSequence body section
     */
    public void addAmqpSequence(AmqpSequence sequence) {
        if (amqpSequence == null)
            amqpSequence = new ArrayList();
        amqpSequence.add(sequence);
        body = null;
        data = null;
        amqpValue = null;
    }

    /**
     * Returns the AmqpValue body section.
     *
     * @return AmqpValue body section
     */
    public AmqpValue getAmqpValue() {
        return amqpValue;
    }

    /**
     * Sets the AmqpValue body section.
     *
     * @param amqpValue AmqpValue body section
     */
    public void setAmqpValue(AmqpValue amqpValue) {
        this.amqpValue = amqpValue;
        body = null;
        data = null;
        amqpSequence = null;
    }

    /**
     * Returns the Footer section.
     *
     * @return Footer section
     */
    public Footer getFooter() {
        return footer;
    }

    /**
     * Sets the Footer section.
     *
     * @param footer Footer section
     */
    public void setFooter(Footer footer) {
        this.footer = footer;
        body = null;
    }

    /**
     * Returns the delivery id.
     *
     * @return delivery id
     */
    public long getDeliveryId() {
        return deliveryId;
    }

    /**
     * Sets the delivery id.
     *
     * @param deliveryId delivery id
     */
    public void setDeliveryId(long deliveryId) {
        this.deliveryId = deliveryId;
    }

    /**
     * <p>
     * Returns whether the message has been settled.
     * </p>
     * A consumer has to check this after receive. If false is returned, the consumer must either call accept() or reject().
     *
     * @return settled or not
     */
    public boolean isSettled() {
        return settled;
    }

    /**
     * Sets whether the message has been settled or not. Internal use only.
     *
     * @param settled settled or not
     */
    public void setSettled(boolean settled) {
        this.settled = settled;
    }

    /**
     * Returns the delivery tag
     *
     * @return deliveryTag
     */
    public DeliveryTag getDeliveryTag() {
        return deliveryTag;
    }

    /**
     * Sets the delivery tag
     *
     * @param deliveryTag
     */
    public void setDeliveryTag(DeliveryTag deliveryTag) {
        this.deliveryTag = deliveryTag;
    }

    private void decode() throws Exception {
        DataInput dataInput = null;
        if (multiBody != null) {
            dataInput = new DataStreamInputStream(new MultiByteArrayInputStream(multiBody, totalSize));
            bodySize = totalSize;
        } else {
            dataInput = new DataByteArrayInputStream(body);
            bodySize = body.length;
        }
        parseSections(dataInput);
    }

    private void parseSections(DataInput dis) throws Exception {
        SectionIF section = SectionFactory.create(AMQPTypeDecoder.decode(dis));
        try {
            while (parseException == null) {
                if (section != null)
                    section.accept(visitor);
                section = SectionFactory.create(AMQPTypeDecoder.decode(dis));
            }
        } catch (EOFException e) {
        }
        if (parseException != null)
            throw parseException;
    }

    private void write(AMQPType t, DataOutput out) throws IOException {
        if (t != null)
            t.writeContent(out);
    }

    /**
     * Writes the content of this message to the data output.
     *
     * @param out data output
     * @throws IOException on error
     */
    public void writeContent(DataOutput out) throws IOException {
        if (body != null)
            out.write(body);
        else {
            write(header, out);
            write(deliveryAnnotations, out);
            write(messageAnnotations, out);
            write(properties, out);
            write(applicationProperties, out);
            if (data != null) {
                for (int i = 0; i < data.size(); i++)
                    ((Data) data.get(i)).writeContent(out);
            } else if (amqpSequence != null) {
                for (int i = 0; i < amqpSequence.size(); i++)
                    ((AmqpSequence) amqpSequence.get(i)).writeContent(out);
            } else if (amqpValue != null) {
                write(amqpValue, out);
            }
            write(footer, out);
        }
    }

    private void appendValue(StringBuffer b, String name, AMQPType t) {
        if (t != null) {
            b.append(", ");
            b.append(name);
            b.append("=");
            b.append(t.getValueString());
        }
    }

    private String getDisplayString() {
        StringBuffer b = new StringBuffer();
        appendValue(b, "header", header);
        appendValue(b, "deliveryAnnotations", deliveryAnnotations);
        appendValue(b, "messageAnnotations", messageAnnotations);
        appendValue(b, "properties", properties);
        appendValue(b, "applicationProperties", applicationProperties);
        if (data != null) {
            for (int i = 0; i < data.size(); i++)
                appendValue(b, "data", (Data) data.get(i));
        } else if (amqpSequence != null) {
            for (int i = 0; i < amqpSequence.size(); i++)
                appendValue(b, "amqpSequence", (AmqpSequence) amqpSequence.get(i));
        } else if (amqpValue != null)
            appendValue(b, "amqpValue", amqpValue);
        appendValue(b, "footer", footer);
        return b.toString();
    }

    public String toString() {
        return "[AMQPMessage" + getDisplayString() + "]";
    }

    private class BodyVisitor extends SectionVisitorAdapter {
        public void visit(Header impl) {
            if (header != null)
                parseException = new Exception("Header already set!");
            header = impl;
        }

        public void visit(DeliveryAnnotations impl) {
            if (deliveryAnnotations != null)
                parseException = new Exception("DeliveryAnnotations already set!");
            deliveryAnnotations = impl;
        }

        public void visit(MessageAnnotations impl) {
            if (messageAnnotations != null)
                parseException = new Exception("MessageAnnotations already set!");
            messageAnnotations = impl;
        }

        public void visit(Properties impl) {
            if (properties != null)
                parseException = new Exception("Properties already set!");
            properties = impl;
        }

        public void visit(ApplicationProperties impl) {
            if (applicationProperties != null)
                parseException = new Exception("ApplicationProperties already set!");
            applicationProperties = impl;
        }

        public void visit(Data impl) {
            if (data == null)
                data = new ArrayList();
            data.add(impl);
        }

        public void visit(AmqpSequence impl) {
            if (amqpSequence == null)
                amqpSequence = new ArrayList();
            amqpSequence.add(impl);
        }

        public void visit(AmqpValue impl) {
            if (amqpValue != null)
                parseException = new Exception("Invalid body part (AmqpSequence), have already a single AmqpValue!");
            else if (data != null)
                parseException = new Exception("Invalid body part (AmqpValue), expecting Data!");
            else if (amqpSequence != null)
                parseException = new Exception("Invalid body part (AmqpValue), expecting AmqpSequence!");
            else {
                amqpValue = impl;
                amqpValue.getValue().resetConstructor();
            }
        }

        public void visit(Footer impl) {
            footer = impl;
        }
    }
}
