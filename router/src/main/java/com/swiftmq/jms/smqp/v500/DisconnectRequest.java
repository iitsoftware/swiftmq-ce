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

package com.swiftmq.jms.smqp.v500;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

public class DisconnectRequest extends Request {
    public DisconnectRequest() {
        super(0, true);
    }

    public int getDumpId() {
        return SMQPFactory.DID_DISCONNECT_REQ;
    }

    protected Reply createReplyInstance() {
        return new DisconnectReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitDisconnectRequest(this);
    }

    public String toString() {
        return "[DisconnectRequest " + super.toString() + "]";
    }

}



