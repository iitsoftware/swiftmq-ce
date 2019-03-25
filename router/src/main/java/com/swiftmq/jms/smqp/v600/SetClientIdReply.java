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

package com.swiftmq.jms.smqp.v600;

/**
 * SMQP-Protocol Version 600, Class: SetClientIdReply
 * Automatically generated, don't change!
 * Generation Date: Thu Feb 09 09:59:46 CET 2006
 * (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.ReplyNE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SetClientIdReply extends ReplyNE {
    private String clientId;

    public SetClientIdReply(String clientId) {
        this.clientId = clientId;
    }

    protected SetClientIdReply() {
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    public int getDumpId() {
        return SMQPFactory.DID_SETCLIENTID_REP;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        if (clientId != null) {
            out.writeBoolean(true);
            SMQPUtil.write(clientId, out);
        } else
            out.writeBoolean(false);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        boolean clientId_set = in.readBoolean();
        if (clientId_set)
            clientId = SMQPUtil.read(clientId, in);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v600/SetClientIdReply, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("clientId=");
        _b.append(clientId);
        _b.append("]");
        return _b.toString();
    }
}
