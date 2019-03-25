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

package com.swiftmq.jms;

import javax.jms.*;
import java.io.Serializable;
import java.util.Enumeration;

public class MessageCloner {
    private static void cloneBytesMessage(BytesMessage source, BytesMessage destination) throws javax.jms.JMSException {
        byte[] buffer = new byte[256];
        try {
            source.reset();
            while (true) {
                int len = source.readBytes(buffer);
                if (len == -1)
                    break;
                destination.writeBytes(buffer, 0, len);
            }
        } catch (MessageEOFException e) {
        }
    }

    private static void cloneMapMessage(MapMessage source, MapMessage destination) throws javax.jms.JMSException {
        for (Enumeration enumer = source.getMapNames(); enumer.hasMoreElements(); ) {
            String name = (String) enumer.nextElement();
            if (source.itemExists(name))
                destination.setObject(name, source.getObject(name));
        }
    }

    private static void cloneObjectMessage(ObjectMessage source, ObjectMessage destination) throws javax.jms.JMSException {
        Serializable obj = source.getObject();
        if (obj != null)
            destination.setObject(obj);
    }

    private static void cloneStreamMessage(StreamMessage source, StreamMessage destination) throws javax.jms.JMSException {
        try {
            source.reset();
            while (true) {
                Object obj = source.readObject();
                if (obj == null)
                    break;
                destination.writeObject(obj);
            }
        } catch (MessageEOFException e) {
        }
    }

    private static void cloneTextMessage(TextMessage source, TextMessage destination) throws javax.jms.JMSException {
        String text = source.getText();
        if (text != null)
            destination.setText(text);
    }

    private static Message cloneContent(Message source, Message destination) throws JMSException {

        // clone message header
        if (source.getJMSCorrelationID() != null)
            destination.setJMSCorrelationID(source.getJMSCorrelationID());
        if (source.getJMSType() != null)
            destination.setJMSType(source.getJMSType());
        destination.setJMSRedelivered(false);
//    destination.setJMSReplyTo(null); // always null (replyTo not supported) / throws exception on Sybase

        // clone message props
        // (don't transfer JMS provider/JMSX props)
        for (Enumeration enumer = source.getPropertyNames(); enumer.hasMoreElements(); ) {
            String name = (String) enumer.nextElement();
            if (!name.toUpperCase().startsWith("JMS_") && !name.toUpperCase().startsWith("JMSX"))
                destination.setObjectProperty(name, source.getObjectProperty(name));
        }

        return destination;
    }

    public static Message cloneMessage(Message source) throws javax.jms.JMSException {
        Message destination = null;

        // clone extending classes
        if (source instanceof BytesMessage) {
            destination = new BytesMessageImpl();
            cloneBytesMessage((BytesMessage) source, (BytesMessage) destination);
        } else if (source instanceof StreamMessage) {
            destination = new StreamMessageImpl();
            cloneStreamMessage((StreamMessage) source, (StreamMessage) destination);
        } else if (source instanceof TextMessage) {
            destination = new TextMessageImpl();
            cloneTextMessage((TextMessage) source, (TextMessage) destination);
        } else if (source instanceof ObjectMessage) {
            destination = new ObjectMessageImpl();
            cloneObjectMessage((ObjectMessage) source, (ObjectMessage) destination);
        } else if (source instanceof MapMessage) {
            destination = new MapMessageImpl();
            cloneMapMessage((MapMessage) source, (MapMessage) destination);
        } else {
            destination = new MessageImpl();
        }

        return cloneContent(source, destination);
    }

    public static Message cloneMessage(Message source, Session session) throws javax.jms.JMSException {
        Message destination = null;

        // clone extending classes
        if (source instanceof BytesMessage) {
            destination = session.createBytesMessage();
            cloneBytesMessage((BytesMessage) source, (BytesMessage) destination);
        } else if (source instanceof StreamMessage) {
            destination = session.createStreamMessage();
            cloneStreamMessage((StreamMessage) source, (StreamMessage) destination);
        } else if (source instanceof TextMessage) {
            destination = session.createTextMessage();
            cloneTextMessage((TextMessage) source, (TextMessage) destination);
        } else if (source instanceof ObjectMessage) {
            destination = session.createObjectMessage();
            cloneObjectMessage((ObjectMessage) source, (ObjectMessage) destination);
        } else if (source instanceof MapMessage) {
            destination = session.createMapMessage();
            cloneMapMessage((MapMessage) source, (MapMessage) destination);
        } else {
            destination = session.createMessage();
        }

        return cloneContent(source, destination);
    }
}
