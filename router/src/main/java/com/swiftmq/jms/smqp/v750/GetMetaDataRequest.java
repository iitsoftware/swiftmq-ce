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
 * SMQP-Protocol Version 750, Class: GetMetaDataRequest
 * Automatically generated, don't change!
 * Generation Date: Tue Apr 21 10:39:21 CEST 2009
 * (c) 2009, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRetryValidator;
import com.swiftmq.tools.requestreply.RequestVisitor;

public class GetMetaDataRequest extends Request {

    public GetMetaDataRequest() {
        super(0, true);
    }

    public GetMetaDataRequest(int dispatchId) {
        super(dispatchId, true);
    }

    public GetMetaDataRequest(RequestRetryValidator validator, int dispatchId) {
        super(dispatchId, true, validator);
    }


    public int getDumpId() {
        return SMQPFactory.DID_GETMETADATA_REQ;
    }


    protected Reply createReplyInstance() {
        return new GetMetaDataReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visit(this);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v750/GetMetaDataRequest, ");
        _b.append(super.toString());
        _b.append("]");
        return _b.toString();
    }
}
