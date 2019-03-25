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

package com.swiftmq.filetransfer.protocol.v941;

import com.swiftmq.filetransfer.protocol.MessageBasedReply;
import com.swiftmq.jms.TextMessageImpl;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.Dom4JDriver;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

public class FileQueryPropsReply extends MessageBasedReply {
    Map<String, Map<String, Object>> result = null;

    public FileQueryPropsReply(Message message) throws JMSException {
        super(message);
        createResult((TextMessage) message);
    }

    public FileQueryPropsReply() {
    }

    private void createResult(TextMessage message) throws JMSException {
        String s = message.getText();
        if (s != null) {
            XStream xStream = new XStream(new Dom4JDriver());
            StringReader reader = new StringReader(s);
            result = (Map<String, Map<String, Object>>) xStream.fromXML(reader);
        }
    }

    public Map<String, Map<String, Object>> getResult() {
        return result;
    }

    public void setResult(Map<String, Map<String, Object>> result) {
        this.result = result;
    }

    public Message toMessage() throws JMSException {
        TextMessage message = new TextMessageImpl();
        message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILEQUERYPROPS_REP);
        if (result != null && result.size() > 0) {
            XStream xStream = new XStream(new Dom4JDriver());
            StringWriter writer = new StringWriter();
            xStream.toXML(result, writer);
            message.setText(writer.toString());
        }
        return fillMessage(message);

    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[FileQueryPropsReply");
        sb.append(super.toString());
        sb.append(" result='").append(result).append('\'');
        sb.append(']');
        return sb.toString();
    }
}
