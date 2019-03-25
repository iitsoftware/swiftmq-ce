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

/*--- formatted by Jindent 2.1, (www.c-lab.de/~jindent) ---*/

package com.swiftmq.jms.smqp.v500;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

/**
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class GetClientIdRequest extends Request {

    /**
     * @SBGen Constructor
     */
    public GetClientIdRequest() {
        super(0, true);
    }

    /**
     * Returns a unique dump id for this object.
     *
     * @return unique dump id
     */
    public int getDumpId() {
        return SMQPFactory.DID_GET_CLIENT_ID_REQ;
    }

    /**
     * @return
     */
    protected Reply createReplyInstance() {
        return new GetClientIdReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitGetClientIdRequest(this);
    }

    /**
     * Method declaration
     *
     * @return
     * @see
     */
    public String toString() {
        return "[GetClientIdRequest " + super.toString() + "]";
    }

}



