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

package com.swiftmq.filetransfer.protocol.v940;

import com.swiftmq.filetransfer.protocol.MessageBasedReply;
import com.swiftmq.jms.MapMessageImpl;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import java.util.ArrayList;
import java.util.List;

public class FileQueryReply extends MessageBasedReply {
    public static final String RESULTSIZE_PROP = "JMS_SWIFTMQ_FT_RESULTSIZE";
    public static final String RESULT_PROP = "JMS_SWIFTMQ_FT_RESULT_";
    List<String> result = null;

    public FileQueryReply(Message message) throws JMSException {
        super(message);
        createResultList((MapMessage) message);
    }

    public FileQueryReply() {
    }

    private void createResultList(MapMessage message) throws JMSException {
        int n = message.getInt(RESULTSIZE_PROP);
        if (n > 0) {
            result = new ArrayList<String>(n);
            for (int i = 0; i < n; i++) {
                result.add(message.getString(new StringBuilder(RESULT_PROP).append(i).toString()));
            }
        }
    }

    public List<String> getResult() {
        return result;
    }

    public void setResult(List<String> result) {
        this.result = result;
    }

    public Message toMessage() throws JMSException {
        MapMessage message = new MapMessageImpl();
        message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILEQUERY_REP);
        if (result == null)
            message.setInt(RESULTSIZE_PROP, 0);
        else {
            message.setInt(RESULTSIZE_PROP, result.size());
            for (int i = 0; i < result.size(); i++) {
                message.setString(new StringBuilder(RESULT_PROP).append(i).toString(), result.get(i));
            }
        }
        return fillMessage(message);

    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[FileChunkReply");
        sb.append(super.toString());
        sb.append(" result='").append(result).append('\'');
        sb.append(']');
        return sb.toString();
    }
}
