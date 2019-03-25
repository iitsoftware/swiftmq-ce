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

import com.swiftmq.filetransfer.protocol.MessageBased;
import com.swiftmq.filetransfer.protocol.MessageBasedFactory;

import javax.jms.JMSException;
import javax.jms.Message;

public class ProtocolFactory implements MessageBasedFactory {
    public static final String DUMPID_PROP = "JMS_SWIFTMQ_FT_DUMPID";
    public static final int FILEPUBLISH_REQ = 0;
    public static final int FILEPUBLISH_REP = 1;
    public static final int FILECONSUME_REQ = 2;
    public static final int FILECONSUME_REP = 3;
    public static final int FILECHUNK_REQ = 4;
    public static final int FILECHUNK_REP = 5;
    public static final int FILEDELETE_REQ = 6;
    public static final int FILEDELETE_REP = 7;
    public static final int FILEQUERY_REQ = 8;
    public static final int FILEQUERY_REP = 9;
    public static final int SESSIONCLOSE_REQ = 10;

    MessageBasedFactory delegatedFactory = null;

    public ProtocolFactory() {
    }

    public ProtocolFactory(MessageBasedFactory delegatedFactory) {
        this.delegatedFactory = delegatedFactory;
    }

    public void setDelegatedFactory(MessageBasedFactory delegatedFactory) {
        this.delegatedFactory = delegatedFactory;
    }

    public MessageBased create(Message message) throws JMSException {
        MessageBased messageBased = null;

        int dumpId = message.getIntProperty(DUMPID_PROP);

        switch (dumpId) {
            case FILEPUBLISH_REQ:
                messageBased = new FilePublishRequest(message);
                break;
            case FILEPUBLISH_REP:
                messageBased = new FilePublishReply(message);
                break;
            case FILECONSUME_REQ:
                messageBased = new FileConsumeRequest(message);
                break;
            case FILECONSUME_REP:
                messageBased = new FileConsumeReply(message);
                break;
            case FILECHUNK_REQ:
                messageBased = new FileChunkRequest(message);
                break;
            case FILECHUNK_REP:
                messageBased = new FileChunkReply(message);
                break;
            case FILEDELETE_REQ:
                messageBased = new FileDeleteRequest(message);
                break;
            case FILEDELETE_REP:
                messageBased = new FileDeleteReply(message);
                break;
            case FILEQUERY_REQ:
                messageBased = new FileQueryRequest(message);
                break;
            case FILEQUERY_REP:
                messageBased = new FileQueryReply(message);
                break;
            case SESSIONCLOSE_REQ:
                messageBased = new SessionCloseRequest(message);
                break;
            default:
                if (delegatedFactory != null)
                    messageBased = delegatedFactory.create(message);
                break;
        }
        return messageBased;
    }
}
