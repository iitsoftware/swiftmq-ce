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
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.impl.amqp.amqp.v01_00_00.Delivery;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class JMSMappingOutboundTransformer extends OutboundTransformer {
    private static final String PROP_BODY_FACTORY = "body-factory";
    private static final String PROP_PREFIX_DA = "prefix-delivery-annotations";
    private static final String PROP_PREFIX_MA = "prefix-message-annotations";
    private static final String PROP_PREFIX_FT = "prefix-footer";
    private static final String PROP_JMS_TYPE = "jms-type-property";

    BodyFactory bodyFactory = null;
    String prefixDA = null;
    String prefixMA = null;
    String prefixFT = null;
    String jmsTypeProp = null;
    DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();

    public void setConfiguration(Map config) throws Exception {
        super.setConfiguration(config);
        bodyFactory = (BodyFactory) Class.forName(getValue(PROP_BODY_FACTORY, "com.swiftmq.impl.amqp.amqp.v01_00_00.transformer.MessageTypeBodyFactory")).newInstance();
        prefixDA = prefixVendor + getValue(PROP_PREFIX_DA, "DA_");
        prefixMA = prefixVendor + getValue(PROP_PREFIX_MA, "MA_");
        prefixFT = prefixVendor + getValue(PROP_PREFIX_FT, "FT_");
    }

    public void transform(Delivery delivery) throws AMQPException, JMSException {
        jmsTypeProp = getValue(PROP_JMS_TYPE, null);
        MessageImpl message = delivery.getMessage();
        if (message.propertyExists(amqpNative) && message.getBooleanProperty(amqpNative))
            throw new JMSException("Message is an AMQP native transformation");

        try {
            AMQPMessage amqpMessage = new AMQPMessage();

            Header header = new Header();
            int deliveryMode = message.getJMSDeliveryMode();
            header.setDurable(new AMQPBoolean(deliveryMode == DeliveryMode.PERSISTENT));
            header.setPriority(new AMQPUnsignedByte(message.getJMSPriority()));
            header.setDeliveryCount(new AMQPUnsignedInt(delivery.getMessageIndex().getDeliveryCount() - 1));
            long ttl = message.getJMSExpiration();
            if (ttl > 0) {
                if (message.propertyExists(Util.PROP_EXPIRATION_CURRENT_TIME_ADD))
                    header.setTtl(new Milliseconds(ttl - message.getLongProperty(Util.PROP_EXPIRATION_CURRENT_TIME_ADD)));
            }

            Properties properties = new Properties();
            String messageId = message.getJMSMessageID();
            if (messageId != null)
                properties.setMessageId(new MessageIdString(message.getJMSMessageID()));
            if (message.propertyExists(Util.PROP_AMQP_TO_ADDRESS))
                properties.setTo(new AddressString(message.getStringProperty(Util.PROP_AMQP_TO_ADDRESS)));
            else
                properties.setTo(new AddressString(message.getJMSDestination().toString()));
            Destination replyTo = message.getJMSReplyTo();
            if (replyTo != null)
                properties.setReplyTo(new AddressString(replyTo.toString()));
            String correlationId = message.getJMSCorrelationID();
            if (correlationId != null)
                properties.setCorrelationId(new MessageIdString(correlationId));
            if (ttl > 0)
                properties.setAbsoluteExpiryTime(new AMQPTimestamp(ttl));
            long timestamp = message.getJMSTimestamp();
            if (timestamp > 0)
                properties.setCreationTime(new AMQPTimestamp(timestamp));

            Map daMap = null;
            Map maMap = null;
            Map ftMap = null;
            Map apMap = null;

            String firstAcquirerName = prefixVendor + "FirstAcquirer";
            String subject = prefixVendor + "Subject";
            String contentType = prefixVendor + "ContentType";
            String contentEncoding = prefixVendor + "ContentEncoding";
            String replyToGroupId = prefixVendor + "ReplyToGroupID";
            for (Enumeration _enum = message.getPropertyNames(); _enum.hasMoreElements(); ) {
                String name = (String) _enum.nextElement();
                if (name.equals(amqpNative) || name.equals(messageFormat) || name.equals(Util.PROP_EXPIRATION_CURRENT_TIME_ADD) || name.equals(Util.PROP_AMQP_TO_ADDRESS))
                    continue;
                if (name.equals(firstAcquirerName))
                    header.setFirstAcquirer(new AMQPBoolean(message.getBooleanProperty(firstAcquirerName)));
                else if (name.equals(MessageImpl.PROP_USER_ID))
                    properties.setUserId(new AMQPBinary(message.getStringProperty(MessageImpl.PROP_USER_ID).getBytes()));
                else if (name.equals(subject))
                    properties.setSubject(new AMQPString(message.getStringProperty(subject)));
                else if (name.equals(contentType))
                    properties.setContentType(new AMQPSymbol(message.getStringProperty(contentType)));
                else if (name.equals(contentEncoding))
                    properties.setContentEncoding(new AMQPSymbol(message.getStringProperty(contentEncoding)));
                else if (name.equals("JMSXGroupID") && message.getObjectProperty(Util.PROP_GROUP_ID) instanceof String)
                    properties.setGroupId(new AMQPString(message.getStringProperty(Util.PROP_GROUP_ID)));
                else if (name.equals("JMSXGroupSeq") && message.getObjectProperty(Util.PROP_GROUP_SEQ) instanceof Long)
                    properties.setGroupSequence(new SequenceNo(message.getLongProperty(Util.PROP_GROUP_SEQ)));
                else if (name.equals(replyToGroupId))
                    properties.setReplyToGroupId(new AMQPString(message.getStringProperty(replyToGroupId)));
                else if (name.startsWith(prefixDA)) {
                    if (daMap == null)
                        daMap = new HashMap();
                    daMap.put(new AMQPSymbol(nameTranslator.translate(name.substring(prefixDA.length()))), Util.convertJMStoAMQP(message.getObjectProperty(name)));
                } else if (name.startsWith(prefixMA)) {
                    if (maMap == null)
                        maMap = new HashMap();
                    maMap.put(new AMQPSymbol(nameTranslator.translate(name.substring(prefixMA.length()))), Util.convertJMStoAMQP(message.getObjectProperty(name)));
                } else if (name.startsWith(prefixFT)) {
                    if (ftMap == null)
                        ftMap = new HashMap();
                    ftMap.put(new AMQPSymbol(nameTranslator.translate(name.substring(prefixFT.length()))), Util.convertJMStoAMQP(message.getObjectProperty(name)));
                } else {
                    if (apMap == null)
                        apMap = new HashMap();
                    apMap.put(new AMQPString(nameTranslator.translate(name)), Util.convertJMStoAMQP(message.getObjectProperty(name)));
                }
            }

            if (message.getJMSType() != null) {
                if (jmsTypeProp != null) {
                    if (apMap == null)
                        apMap = new HashMap();
                    apMap.put(new AMQPString(jmsTypeProp), Util.convertJMStoAMQP(message.getJMSType()));
                } else {
                    if (maMap == null)
                        maMap = new HashMap();
                    maMap.put(new AMQPSymbol("x-opt-jms-type"), new AMQPString(message.getJMSType()));
                }
            }
            amqpMessage.setHeader(header);
            amqpMessage.setProperties(properties);
            if (daMap != null)
                amqpMessage.setDeliveryAnnotations(new DeliveryAnnotations(daMap));
            if (maMap != null)
                amqpMessage.setMessageAnnotations(new MessageAnnotations(maMap));
            if (apMap != null)
                amqpMessage.setApplicationProperties(new ApplicationProperties(apMap));
            if (ftMap != null)
                amqpMessage.setFooter(new Footer(ftMap));
            bodyFactory.createBody(message, amqpMessage);
            delivery.setAmqpMessage(amqpMessage);
            dbos.rewind();
            amqpMessage.writeContent(dbos);
            byte[] payload = new byte[dbos.getCount()];
            System.arraycopy(dbos.getBuffer(), 0, payload, 0, dbos.getCount());
            delivery.setData(payload);
            delivery.setMessageFormat(0);
        } catch (IOException e) {
            e.printStackTrace();
            throw new AMQPException(e.toString());
        }
    }
}
