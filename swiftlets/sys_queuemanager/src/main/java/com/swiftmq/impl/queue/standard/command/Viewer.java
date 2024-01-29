/*
 * Copyright 2024 IIT Software GmbH
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

package com.swiftmq.impl.queue.standard.command;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.impl.queue.standard.queue.StoreId;
import com.swiftmq.jms.*;
import com.swiftmq.mgmt.Command;
import com.swiftmq.mgmt.CommandExecutor;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.TreeCommands;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.MessageEntry;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import javax.jms.DeliveryMode;
import javax.jms.MessageEOFException;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Viewer
        implements CommandExecutor {
    static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd MMM yyyy HH:mm:ss.SSS Z");
    static final int MAX_RESULT = 100;
    static String COMMAND = "view";
    static String PATTERN = "view <queue> <start> (<stop>|*) [<selector>] [truncate <kb>]";
    static String DESCRIPTION = "View Messages";
    SwiftletContext ctx = null;
    DecimalFormat fmt = null;

    public Viewer(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public Command createCommand() {
        return new Command(COMMAND, PATTERN, DESCRIPTION, true, this, true, true);
    }

    private ViewCollector createCollector(String queueName, String selectorString, int from) throws Exception {
        if (!ctx.queueManager.isQueueDefined(queueName))
            throw new Exception("Unknown queue: " + queueName);
        MessageSelector selector = null;
        if (selectorString != null) {
            selector = new MessageSelector(selectorString);
            selector.compile();
        }
        return new ViewCollector(ctx.queueManager.getQueueForInternalUse(queueName), from, selector, MAX_RESULT);
    }

    private String replaceInvalidXMLChars(String in) {
        StringBuffer out = new StringBuffer();
        char c;

        if (in == null || "".equals(in))
            return "";

        for (int i = 0; i < in.length(); i++) {
            c = in.charAt(i);
            if (c == 0x9 || c == 0xA || c == 0xD || c >= 0x20 && c <= 0xD7FF || c >= 0xE000 && c <= 0xFFFD)
                out.append(c);
            else
                out.append("?");
        }

        return out.toString();
    }

    private Element createElement(String name, String value) throws Exception {
        Element element = DocumentHelper.createElement(name);
        element.setText(replaceInvalidXMLChars(value));
        return element;
    }

    private Element createPropertyElement(String elementName, String name, Object value, Class clazz, int truncSize) throws Exception {
        Element element = DocumentHelper.createElement(elementName);
        element.addAttribute("name", name);
        element.addAttribute("type", clazz.getName());
        if (value instanceof byte[]) {
            byte[] buff = (byte[]) value;
            StringBuffer b = new StringBuffer("{");
            for (int i = 0; i < Math.min(buff.length, truncSize); i++) {
                if (i > 0)
                    b.append(",");
                b.append(buff[i] & 0xff);
            }
            if (buff.length > truncSize)
                b.append("...(truncated)");
            b.append("}");
            element.addAttribute("value", b.toString());
        } else if (value instanceof Byte)
            element.addAttribute("value", String.valueOf((Byte) value & 0xff));
        else
            element.addAttribute("value", value.toString());
        return element;
    }

    private void createJMSHeaderOutput(MessageImpl message, Element parent) throws Exception {
        Element header = DocumentHelper.createElement("jms-header");
        header.add(createElement("JMSDeliveryMode", message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT"));
        if (message.getJMSCorrelationID() != null)
            header.add(createElement("JMSCorrelationID", message.getJMSCorrelationID()));
        header.add(createElement("JMSDestination", message.getJMSDestination().toString()));
        long exp = message.getJMSExpiration();
        if (exp > 0)
            header.add(createElement("JMSExpiration", DATE_FORMAT.format(new Date(exp)) + " (" + exp + ")"));
        if (message.getJMSMessageID() != null)
            header.add(createElement("JMSMessageID", message.getJMSMessageID()));
        header.add(createElement("JMSPriority", String.valueOf(message.getJMSPriority())));
        header.add(createElement("JMSRedelivered", String.valueOf(message.getJMSRedelivered())));
        if (message.getJMSReplyTo() != null)
            header.add(createElement("JMSReplyTo", message.getJMSReplyTo().toString()));
        long ts = message.getJMSTimestamp();
        if (ts > 0)
            header.add(createElement("JMSTimestamp", DATE_FORMAT.format(new Date(ts)) + " (" + ts + ")"));
        if (message.getJMSType() != null)
            header.add(createElement("JMSType", message.getJMSType()));
        parent.add(header);
    }

    private void createJMSXHeaderOutput(MessageEntry entry, Element parent) throws Exception {
        Element header = DocumentHelper.createElement("jmsx-header");
        for (Enumeration _enum = entry.getMessage().getPropertyNames(); _enum.hasMoreElements(); ) {
            String name = (String) _enum.nextElement();
            if (name.startsWith("JMSX")) {
                Object value = entry.getMessage().getObjectProperty(name);
                header.add(createElement(name, value.toString()));
            }
        }
        if (entry.getMessage().getStringProperty(MessageImpl.PROP_USER_ID) != null)
            header.add(createElement(MessageImpl.PROP_USER_ID, entry.getMessage().getStringProperty(MessageImpl.PROP_USER_ID)));

        header.add(createElement(MessageImpl.PROP_DELIVERY_COUNT, String.valueOf(entry.getMessageIndex().getDeliveryCount())));
        parent.add(header);
    }

    private void createJMSVendorHeaderOutput(MessageImpl message, Element parent) throws Exception {
        Element header = DocumentHelper.createElement("jms-vendor-properties");
        for (Enumeration _enum = message.getPropertyNames(); _enum.hasMoreElements(); ) {
            String name = (String) _enum.nextElement();
            if (name.startsWith("JMS_")) {
                Object value = message.getObjectProperty(name);
                header.add(createPropertyElement("property", name, value, value.getClass(), 2048));
            }
        }
        parent.add(header);
    }

    private void createMessagePropertyOutput(MessageImpl message, Element parent) throws Exception {
        Element header = DocumentHelper.createElement("message-properties");
        for (Enumeration _enum = message.getPropertyNames(); _enum.hasMoreElements(); ) {
            String name = (String) _enum.nextElement();
            if (!(name.startsWith("JMS_") || name.startsWith("JMSX"))) {
                Object value = message.getObjectProperty(name);
                header.add(createPropertyElement("property", name, value, value.getClass(), 2048));
            }
        }
        parent.add(header);
    }

    private void createHeaderOutput(MessageEntry entry, Element parent) throws Exception {
        createJMSHeaderOutput(entry.getMessage(), parent);
        createJMSXHeaderOutput(entry, parent);
        createJMSVendorHeaderOutput(entry.getMessage(), parent);
        createMessagePropertyOutput(entry.getMessage(), parent);
    }

    private void createTextMessageOutput(TextMessageImpl message, MessageEntry entry, Element parent, int truncSize) throws Exception {
        parent.addAttribute("type", "TextMessage");
        createHeaderOutput(entry, parent);
        Element element = DocumentHelper.createElement("body");
        String s = message.getText();
        if (s == null)
            s = "body is null";
        else if (s.length() > truncSize)
            s = s.substring(0, truncSize) + "...(truncated)";
        element.setText(replaceInvalidXMLChars(s));
        parent.add(element);
    }

    private Element createStreamElement(Object obj, int truncSize) throws Exception {
        Element element = DocumentHelper.createElement("stream-item");
        element.addAttribute("type", obj.getClass().getName());
        if (obj instanceof byte[]) {
            byte[] buff = (byte[]) obj;
            StringBuffer b = new StringBuffer("{");
            for (int i = 0; i < Math.min(buff.length, truncSize); i++) {
                if (i > 0)
                    b.append(",");
                b.append(buff[i] & 0xff);
            }
            if (buff.length > truncSize)
                b.append("...(truncated)");
            b.append("}");
            element.addAttribute("value", b.toString());
        } else if (obj instanceof Byte)
            element.addAttribute("value", String.valueOf((Byte) obj & 0xff));
        else
            element.addAttribute("value", obj.toString());
        return element;
    }

    private void createStreamMessageOutput(StreamMessageImpl message, MessageEntry entry, Element parent, int truncSize) throws Exception {
        parent.addAttribute("type", "StreamMessage");
        createHeaderOutput(entry, parent);
        Element element = DocumentHelper.createElement("body");
        try {
            message.reset();
            while (true) {
                Object obj = message.readObject();
                if (obj == null)
                    break;
                element.add(createStreamElement(obj, truncSize));
            }
        } catch (MessageEOFException e) {
        }
        parent.add(element);
    }

    private void createBytesMessageOutput(BytesMessageImpl message, MessageEntry entry, Element parent, int truncSize) throws Exception {
        parent.addAttribute("type", "BytesMessage");
        createHeaderOutput(entry, parent);
        message.reset();
        Element element = DocumentHelper.createElement("body");
        element.addAttribute("body-length", String.valueOf(message.getBodyLength()));
        try {
            StringBuffer b = new StringBuffer("{");
            for (int i = 0; i < Math.min(message.getBodyLength(), truncSize); i++) {
                if (i > 0)
                    b.append(",");
                b.append(message.readUnsignedByte());
            }
            if (message.getBodyLength() > truncSize)
                b.append("...(truncated)");
            b.append("}");
            element.setText(replaceInvalidXMLChars(b.toString()));
        } catch (MessageEOFException e) {
        }
        parent.add(element);
    }

    private void createMapMessageOutput(MapMessageImpl message, MessageEntry entry, Element parent, int truncSize) throws Exception {
        parent.addAttribute("type", "MapMessage");
        createHeaderOutput(entry, parent);
        Element element = DocumentHelper.createElement("body");
        for (Enumeration _enum = message.getMapNames(); _enum.hasMoreElements(); ) {
            String name = (String) _enum.nextElement();
            Object value = message.getObject(name);
            if (value == null)
                value = "null";
            element.add(createPropertyElement("item", name, value, value.getClass(), truncSize));
        }
        parent.add(element);
    }

    private void createObjectMessageOutput(ObjectMessageImpl message, MessageEntry entry, Element parent, int truncSize) throws Exception {
        parent.addAttribute("type", "ObjectMessage");
        createHeaderOutput(entry, parent);
        Element element = DocumentHelper.createElement("body");
        Object obj = null;
        String s = null;
        try {
            obj = message.getObject();
            if (obj == null)
                s = "null";
            else {
                s = obj.toString();
                if (s.length() > truncSize)
                    s = s.substring(0, truncSize) + "...(truncated)";
            }
        } catch (Exception e) {
            s = "*** Unable to display object.toString() *** [Exception was: " + e.toString() + "]";
        }
        element.setText(replaceInvalidXMLChars(s));
        parent.add(element);
    }

    private void createMessageOutput(MessageImpl message, MessageEntry entry, Element parent, int truncSize) throws Exception {
        parent.addAttribute("type", "Message");
        createHeaderOutput(entry, parent);
        Element element = DocumentHelper.createElement("body");
        element.setText("Message has no body");
    }

    private void createMessageOutput(MessageEntry entry, Element parent, int idx, int truncSize) throws Exception {
        StoreId storeId = (StoreId) entry.getMessageIndex();
        Element element = DocumentHelper.createElement("message");
        element.addAttribute("index", String.valueOf(idx));
        element.addAttribute("message-key", String.valueOf(entry.getMessageIndex().getId()));
        element.addAttribute("locked", Boolean.valueOf(storeId.isLocked()).toString());
        MessageImpl msg = entry.getMessage();
        element.addAttribute("size", String.valueOf(msg.getMessageLength()));
        if (msg instanceof TextMessageImpl)
            createTextMessageOutput((TextMessageImpl) msg, entry, element, truncSize);
        else if (msg instanceof ObjectMessageImpl)
            createObjectMessageOutput((ObjectMessageImpl) msg, entry, element, truncSize);
        else if (msg instanceof MapMessageImpl)
            createMapMessageOutput((MapMessageImpl) msg, entry, element, truncSize);
        else if (msg instanceof StreamMessageImpl)
            createStreamMessageOutput((StreamMessageImpl) msg, entry, element, truncSize);
        else if (msg instanceof BytesMessageImpl)
            createBytesMessageOutput((BytesMessageImpl) msg, entry, element, truncSize);
        else
            createMessageOutput(msg, entry, element, truncSize);
        parent.add(element);
    }

    private String[] docToArray(Document doc) throws Exception {
        StringWriter sw = new StringWriter();
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setLineSeparator("\n");
        format.setNewlines(true);
        XMLWriter writer = new XMLWriter(sw, format);
        writer.write(doc);
        writer.flush();
        writer.close();
        List<String> list = new ArrayList<>();
        list.add(TreeCommands.RESULT);
        StringTokenizer t = new StringTokenizer(sw.toString(), "\n");
        while (t.hasMoreTokens())
            list.add(t.nextToken());
        return list.toArray(new String[0]);
    }

    private String[] createResult(ViewCollector collector, int from, int truncSize) throws Exception {
        Document doc = DocumentHelper.createDocument();
        Element root = DocumentHelper.createElement("result");
        ViewEntry entry = null;
        while ((entry = collector.next()) != null) {
            createMessageOutput(entry.messageEntry, root, entry.index, truncSize);
        }
        doc.setRootElement(root);
        doc.addComment(" " + collector.resultSize() + " messages displayed, " + collector.queueSize() + " messages total in queue ");
        doc.addComment(" If necessary, message body [partly] truncated to " + truncSize + " characters ");
        return docToArray(doc);
    }

    public String[] execute(String[] context, Entity entity, String[] cmd) {
        if (cmd.length < 4 || cmd.length > 7)
            return new String[]{TreeCommands.ERROR, "Invalid command, please try 'view <queue> <startidx> (<stopidx>|*) [<selector>] [truncate <n>]'"};
        String[] result = null;
        try {
            int from = Integer.parseInt(cmd[2]);
            if (from < 0)
                throw new Exception("<startidx> must be >= 0");
            int truncSize = 2048;
            boolean fail = false;
            String selector = null;
            if (cmd.length > 4) {
                if (cmd[4].equals("truncate")) {
                    if (cmd.length == 6)
                        truncSize = Integer.parseInt(cmd[5]);
                    else
                        fail = true;
                } else if (cmd.length == 7) {
                    selector = cmd[4];
                    if (cmd[5].equals("truncate"))
                        truncSize = Integer.parseInt(cmd[6]);
                    else
                        fail = true;
                } else if (cmd.length == 5)
                    selector = cmd[4];
                else
                    fail = true;
            }
            if (fail)
                throw new Exception("Invalid command, please try 'view <queue> <startidx> (<stopidx>|*) [<selector>] [truncate <n>]'");
            if (truncSize < 1)
                throw new Exception("Invalid truncate size: " + truncSize);
            ViewCollector collector = createCollector(cmd[1], selector, from);
            result = createResult(collector, from, truncSize);
            collector.close();
        } catch (Exception e) {
            result = new String[]{TreeCommands.ERROR, e.getMessage()};
        }
        return result;
    }
}

