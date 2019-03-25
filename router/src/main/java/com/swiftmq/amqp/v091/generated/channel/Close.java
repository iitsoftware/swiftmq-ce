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

package com.swiftmq.amqp.v091.generated.channel;

/**
 * AMQP-Protocol Version 091
 * Automatically generated, don't change!
 * Generation Date: Thu Apr 12 12:18:24 CEST 2012
 * (c) 2012, IIT Software GmbH, Bremen/Germany
 * All Rights Reserved
 **/

import com.swiftmq.amqp.v091.io.BitSupportDataInput;
import com.swiftmq.amqp.v091.io.BitSupportDataOutput;
import com.swiftmq.amqp.v091.types.Coder;

import java.io.IOException;

public class Close extends ChannelMethod {
    int replyCode;
    String replyText;
    int classId;
    int methodId;

    public Close() {
        _classId = 20;
        _methodId = 40;
    }

    public void accept(ChannelMethodVisitor visitor) {
        visitor.visit(this);
    }

    public int getReplyCode() {
        return replyCode;
    }

    public void setReplyCode(int replyCode) {
        this.replyCode = replyCode;
    }

    public String getReplyText() {
        return replyText;
    }

    public void setReplyText(String replyText) {
        this.replyText = replyText;
    }

    public int getClassId() {
        return classId;
    }

    public void setClassId(int classId) {
        this.classId = classId;
    }

    public int getMethodId() {
        return methodId;
    }

    public void setMethodId(int methodId) {
        this.methodId = methodId;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        replyCode = Coder.readShort(in);
        replyText = Coder.readShortString(in);
        classId = Coder.readShort(in);
        methodId = Coder.readShort(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeShort(replyCode, out);
        Coder.writeShortString(replyText, out);
        Coder.writeShort(classId, out);
        Coder.writeShort(methodId, out);
        out.bitFlush();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer(" ");
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("replyCode=");
        b.append(replyCode);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("replyText=");
        b.append(replyText);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("classId=");
        b.append(classId);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("methodId=");
        b.append(methodId);
        return b.toString();
    }

    public String toString() {
        return "[Close " + super.toString() + getDisplayString() + "]";
    }
}
