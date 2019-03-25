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
 * SMQP-Protocol Version 610, Class: XAResStartReply
 * Automatically generated, don't change!
 * Generation Date: Mon Jul 17 17:50:11 CEST 2006
 * (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.ReplyNE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class XAResStartReply extends ReplyNE {
    private int errorCode;

    public XAResStartReply(int errorCode) {
        this.errorCode = errorCode;
    }

    protected XAResStartReply() {
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public int getDumpId() {
        return SMQPFactory.DID_XARESSTART_REP;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        SMQPUtil.write(errorCode, out);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        errorCode = SMQPUtil.read(errorCode, in);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v610/XAResStartReply, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("errorCode=");
        _b.append(errorCode);
        _b.append("]");
        return _b.toString();
    }
}
