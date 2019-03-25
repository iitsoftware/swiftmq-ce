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

package com.swiftmq.jms.smqp.v750;

/**
 * SMQP-Protocol Version 750, Class: FetchBrowserMessageReply
 * Automatically generated, don't change!
 * Generation Date: Tue Apr 21 10:39:21 CEST 2009
 * (c) 2009, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.tools.requestreply.ReplyNE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FetchBrowserMessageReply extends ReplyNE {
    private MessageEntry messageEntry;

    public FetchBrowserMessageReply(MessageEntry messageEntry) {
        this.messageEntry = messageEntry;
    }

    protected FetchBrowserMessageReply() {
    }

    public void setMessageEntry(MessageEntry messageEntry) {
        this.messageEntry = messageEntry;
    }

    public MessageEntry getMessageEntry() {
        return messageEntry;
    }

    public int getDumpId() {
        return SMQPFactory.DID_FETCHBROWSERMESSAGE_REP;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        if (messageEntry != null) {
            out.writeBoolean(true);
            SMQPUtil.write(messageEntry, out);
        } else
            out.writeBoolean(false);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        boolean messageEntry_set = in.readBoolean();
        if (messageEntry_set)
            messageEntry = SMQPUtil.read(messageEntry, in);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v750/FetchBrowserMessageReply, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("messageEntry=");
        _b.append(messageEntry);
        _b.append("]");
        return _b.toString();
    }
}
