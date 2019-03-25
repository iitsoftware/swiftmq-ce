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

package com.swiftmq.impl.amqp.amqp.v01_00_00.transformer;

import com.swiftmq.amqp.v100.client.AMQPException;
import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.Milliseconds;
import com.swiftmq.amqp.v100.generated.transport.definitions.SequenceNo;
import com.swiftmq.amqp.v100.generated.transport.performatives.TransferFrame;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.jms.MessageImpl;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JMSMappingInboundTransformer extends InboundTransformer {
    private static final String PROP_MESSAGE_FACTORY = "message-factory";
    private static final String PROP_PREFIX_DA = "prefix-delivery-annotations";
    private static final String PROP_PREFIX_MA = "prefix-message-annotations";
    private static final String PROP_PREFIX_FT = "prefix-footer";
    private static final String PROP_OVERWRITE_MESSAGE_ID = "overwrite-message-id";
    private static final String PROP_JMS_TYPE = "jms-type-property";

    MessageFactory messageFactory = null;
    String prefixDA = null;
    String prefixMA = null;
    String prefixFT = null;
    boolean overwriteMessageId;
    String jmsTypeProp = null;

    public void setConfiguration(Map config) throws Exception {
        super.setConfiguration(config);

        messageFactory = (MessageFactory) Class.forName(getValue(PROP_MESSAGE_FACTORY, "com.swiftmq.impl.amqp.amqp.v01_00_00.transformer.BodyTypeMessageFactory")).newInstance();
        prefixDA = prefixVendor + getValue(PROP_PREFIX_DA, "DA_");
        prefixMA = prefixVendor + getValue(PROP_PREFIX_MA, "MA_");
        prefixFT = prefixVendor + getValue(PROP_PREFIX_FT, "FT_");
        overwriteMessageId = Boolean.parseBoolean(getValue(PROP_OVERWRITE_MESSAGE_ID, "false"));
    }

    protected void transformHeader(Header header, MessageImpl jmsMessage) throws Exception {
        AMQPBoolean durable = header.getDurable();
        if (durable != null) {
            if (durable.getValue())
                jmsMessage.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            else
                jmsMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        } else
            jmsMessage.setJMSDeliveryMode(defaultDeliveryMode);

        AMQPUnsignedByte priority = header.getPriority();
        if (priority != null)
            jmsMessage.setJMSPriority(priority.getValue());
        else
            jmsMessage.setJMSPriority(defaultPriority);

        Milliseconds ttl = header.getTtl();
        if (ttl != null) {
            long lTTL = ttl.getValue();
            if (lTTL > 0) {
                long current = System.currentTimeMillis();
                jmsMessage.setJMSExpiration(current + lTTL);
                jmsMessage.setLongProperty(Util.PROP_EXPIRATION_CURRENT_TIME_ADD, current);
            }
        } else {
            if (defaultTtl > 0)
                jmsMessage.setJMSExpiration(System.currentTimeMillis() + defaultTtl);
        }

        AMQPBoolean firstAcquirer = header.getFirstAcquirer();
        if (firstAcquirer != null)
            jmsMessage.setBooleanProperty(prefixVendor + "FirstAcquirer", firstAcquirer.getValue());
    }

    protected void transformMap(Map map, MessageImpl jmsMessage, String prefix) throws Exception {
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
            boolean isJMSType = false;
            Map.Entry entry = (Map.Entry) iter.next();
            AMQPType key = (AMQPType) entry.getKey();
            AMQPDescribedConstructor constructor = key.getConstructor();
            int code = constructor != null ? constructor.getFormatCode() : key.getCode();
            String propName = null;
            if (AMQPTypeDecoder.isULong(code))
                propName = prefix + String.valueOf(((AMQPUnsignedLong) key).getValue());
            else if (AMQPTypeDecoder.isSymbol(code)) {
                if (prefix.equals(prefixMA) && ((AMQPSymbol) key).getValue().equals("x-opt-jms-type"))
                    isJMSType = true;
                propName = prefix + nameTranslator.translate(((AMQPSymbol) key).getValue());
            }

            if (isJMSType) {
                AMQPType t = (AMQPType) entry.getValue();
                if (AMQPTypeDecoder.isString(t.getCode()))
                    jmsMessage.setJMSType(((AMQPString) entry.getValue()).getValue());
                else
                    jmsMessage.setStringProperty("JMS_TYPE_ERROR", "x-opt-jms-type must be of String value");
            } else if (propName != null) {
                try {
                    jmsMessage.setObjectProperty(propName, Util.convertAMQPtoJMS((AMQPType) entry.getValue()));
                } catch (Exception e) {
                    jmsMessage.setStringProperty(propName + "_ERROR", e.getMessage());
                }
            }
        }
    }

    protected void transformProperties(Properties properties, final MessageImpl jmsMessage, final DestinationFactory destinationFactory) throws Exception {
        if (overwriteMessageId) {
            jmsMessage.setJMSMessageID(nextMsgId());
        } else {
            MessageIdIF messageIdIF = properties.getMessageId();
            if (messageIdIF != null)
                messageIdIF.accept(new MessageIdVisitor() {
                    public void visit(MessageIdUlong messageIdUlong) {
                        try {
                            jmsMessage.setJMSMessageID(String.valueOf(messageIdUlong.getValue()));
                        } catch (JMSException e) {
                        }
                    }

                    public void visit(MessageIdUuid messageIdUuid) {
                        try {
                            jmsMessage.setJMSMessageID(String.valueOf(messageIdUuid.getValue()));
                        } catch (JMSException e) {
                        }
                    }

                    public void visit(MessageIdBinary messageIdBinary) {
                        try {
                            jmsMessage.setJMSMessageID(new String(messageIdBinary.getValue()));
                        } catch (JMSException e) {
                        }
                    }

                    public void visit(MessageIdString messageIdString) {
                        try {
                            jmsMessage.setJMSMessageID(messageIdString.getValue());
                        } catch (JMSException e) {
                        }
                    }
                });
        }

        AMQPBinary userId = properties.getUserId();
        if (userId != null)
            jmsMessage.setStringProperty(MessageImpl.PROP_USER_ID, new String(userId.getValue(), "ISO-8859-1"));

        AddressIF to = properties.getTo();
        if (to != null) {
            jmsMessage.setJMSDestination(destinationFactory.create(to));
            to.accept(new AddressVisitor() {
                public void visit(AddressString addressString) {
                    try {
                        jmsMessage.setStringProperty(Util.PROP_AMQP_TO_ADDRESS, addressString.getValue());
                    } catch (JMSException e) {
                    }
                }
            });
        }

        AMQPString subject = properties.getSubject();
        if (subject != null)
            jmsMessage.setStringProperty(prefixVendor + "Subject", subject.getValue());

        AddressIF replyTo = properties.getReplyTo();
        if (replyTo != null)
            jmsMessage.setJMSReplyTo(destinationFactory.create(replyTo));

        MessageIdIF correlationIdIF = properties.getCorrelationId();
        if (correlationIdIF != null)
            correlationIdIF.accept(new MessageIdVisitor() {
                public void visit(MessageIdUlong messageIdUlong) {
                    try {
                        jmsMessage.setJMSCorrelationID(String.valueOf(messageIdUlong.getValue()));
                    } catch (JMSException e) {
                    }
                }

                public void visit(MessageIdUuid messageIdUuid) {
                    try {
                        jmsMessage.setJMSCorrelationID(String.valueOf(messageIdUuid.getValue()));
                    } catch (JMSException e) {
                    }
                }

                public void visit(MessageIdBinary messageIdBinary) {
                    try {
                        jmsMessage.setJMSCorrelationID(new String(messageIdBinary.getValue()));
                    } catch (JMSException e) {
                    }
                }

                public void visit(MessageIdString messageIdString) {
                    try {
                        jmsMessage.setJMSCorrelationID(messageIdString.getValue());
                    } catch (JMSException e) {
                    }
                }
            });

        AMQPSymbol contentType = properties.getContentType();
        if (contentType != null)
            jmsMessage.setStringProperty(prefixVendor + "ContentType", contentType.getValue());

        AMQPSymbol contentEncoding = properties.getContentEncoding();
        if (contentEncoding != null)
            jmsMessage.setStringProperty(prefixVendor + "ContentEncoding", contentEncoding.getValue());

        AMQPTimestamp ts = properties.getAbsoluteExpiryTime();
        if (ts != null)
            jmsMessage.setJMSExpiration(ts.getValue());

        AMQPTimestamp ct = properties.getCreationTime();
        if (ct != null)
            jmsMessage.setJMSTimestamp(ct.getValue());

        AMQPString groupId = properties.getGroupId();
        if (groupId != null)
            jmsMessage.setStringProperty(Util.PROP_GROUP_ID, groupId.getValue());

        SequenceNo groupSeq = properties.getGroupSequence();
        if (groupSeq != null)
            jmsMessage.setLongProperty(Util.PROP_GROUP_SEQ, groupSeq.getValue());

        AMQPString replyToGroupId = properties.getReplyToGroupId();
        if (replyToGroupId != null)
            jmsMessage.setStringProperty(prefixVendor + "ReplyToGroupID", replyToGroupId.getValue());
    }

    protected void transformApplicationProperties(ApplicationProperties applicationProperties, MessageImpl jmsMessage) throws Exception {
        Map map = applicationProperties.getValue();
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry entry = (Map.Entry) iter.next();
            AMQPType key = (AMQPType) entry.getKey();
            AMQPDescribedConstructor constructor = key.getConstructor();
            int code = constructor != null ? constructor.getFormatCode() : key.getCode();
            String propName = null;
            if (AMQPTypeDecoder.isString(code))
                propName = nameTranslator.translate(((AMQPString) key).getValue());
            else if (AMQPTypeDecoder.isSymbol(code))
                propName = nameTranslator.translate(((AMQPSymbol) key).getValue());

            if (propName != null) {
                if (jmsTypeProp != null && propName.equals(jmsTypeProp)) {
                    AMQPType t = (AMQPType) entry.getValue();
                    if (AMQPTypeDecoder.isString(t.getCode()))
                        jmsMessage.setJMSType(((AMQPString) entry.getValue()).getValue());
                    else
                        jmsMessage.setStringProperty("JMS_TYPE_ERROR", jmsTypeProp + " must be of String value");
                } else {
                    try {
                        jmsMessage.setObjectProperty(propName, Util.convertAMQPtoJMS((AMQPType) entry.getValue()));
                    } catch (Exception e) {
                        jmsMessage.setStringProperty(propName + "_ERROR", e.getMessage());
                    }
                }
            }
        }
    }

    public MessageImpl transform(TransferFrame frame, DestinationFactory destinationFactory) throws AMQPException, JMSException {
        jmsTypeProp = getValue(PROP_JMS_TYPE, null);
        MessageImpl jmsMessage = null;
        try {
            AMQPMessage amqpMessage = null;
            if (frame.getMorePayloads() != null) {
                List morePayloads = frame.getMorePayloads();
                byte[][] b = new byte[morePayloads.size() + 1][];
                b[0] = frame.getPayload();
                for (int i = 0; i < morePayloads.size(); i++)
                    b[i + 1] = (byte[]) morePayloads.get(i);
                amqpMessage = new AMQPMessage(b, frame.getPayloadLength());
            } else
                amqpMessage = new AMQPMessage(frame.getPayload());
            jmsMessage = messageFactory.create(amqpMessage);
            jmsMessage.setLongProperty(prefixVendor + Util.PROP_MESSAGE_FORMAT, frame.getMessageFormat().getValue());
            jmsMessage.setBooleanProperty(amqpNative, false);
            Header header = amqpMessage.getHeader();
            if (header != null)
                transformHeader(header, jmsMessage);
            DeliveryAnnotations deliveryAnnotations = amqpMessage.getDeliveryAnnotations();
            if (deliveryAnnotations != null)
                transformMap(deliveryAnnotations.getValue(), jmsMessage, prefixDA);
            MessageAnnotations messageAnnotations = amqpMessage.getMessageAnnotations();
            if (messageAnnotations != null)
                transformMap(messageAnnotations.getValue(), jmsMessage, prefixMA);
            Properties properties = amqpMessage.getProperties();
            if (properties != null)
                transformProperties(properties, jmsMessage, destinationFactory);
            ApplicationProperties applicationProperties = amqpMessage.getApplicationProperties();
            if (applicationProperties != null)
                transformApplicationProperties(applicationProperties, jmsMessage);
            Footer footer = amqpMessage.getFooter();
            if (footer != null)
                transformMap(footer.getValue(), jmsMessage, prefixFT);
        } catch (Exception e) {
            throw new AMQPException(e.toString());
        }
        jmsMessage.reset();
        return jmsMessage;
    }
}
