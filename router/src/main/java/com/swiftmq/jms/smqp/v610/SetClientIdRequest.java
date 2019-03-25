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

package com.swiftmq.jms.smqp.v610;

/**
 * SMQP-Protocol Version 610, Class: SetClientIdRequest
 * Automatically generated, don't change!
 * Generation Date: Mon Jul 17 17:50:10 CEST 2006
 * (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRetryValidator;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SetClientIdRequest extends Request {
    private String clientId;

    public SetClientIdRequest() {
        super(0, true);
    }

    public SetClientIdRequest(int dispatchId) {
        super(dispatchId, true);
    }

    public SetClientIdRequest(RequestRetryValidator validator, int dispatchId) {
        super(dispatchId, true, validator);
    }

    public SetClientIdRequest(int dispatchId, String clientId) {
        super(dispatchId, true);
        this.clientId = clientId;
    }

    public SetClientIdRequest(RequestRetryValidator validator, int dispatchId, String clientId) {
        super(dispatchId, true, validator);
        this.clientId = clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    public int getDumpId() {
        return SMQPFactory.DID_SETCLIENTID_REQ;
    }


    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        SMQPUtil.write(clientId, out);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        clientId = SMQPUtil.read(clientId, in);
    }

    protected Reply createReplyInstance() {
        return new SetClientIdReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visit(this);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v610/SetClientIdRequest, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("clientId=");
        _b.append(clientId);
        _b.append("]");
        return _b.toString();
    }
}
