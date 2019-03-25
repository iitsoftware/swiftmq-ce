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

package com.swiftmq.amqp.v091.generated.queue;

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

public class Delete extends QueueMethod {
    int reserved1;
    String queue;
    boolean ifUnused;
    boolean ifEmpty;
    boolean noWait;

    public Delete() {
        _classId = 50;
        _methodId = 40;
    }

    public void accept(QueueMethodVisitor visitor) {
        visitor.visit(this);
    }

    public int getReserved1() {
        return reserved1;
    }

    public void setReserved1(int reserved1) {
        this.reserved1 = reserved1;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public boolean getIfUnused() {
        return ifUnused;
    }

    public void setIfUnused(boolean ifUnused) {
        this.ifUnused = ifUnused;
    }

    public boolean getIfEmpty() {
        return ifEmpty;
    }

    public void setIfEmpty(boolean ifEmpty) {
        this.ifEmpty = ifEmpty;
    }

    public boolean getNoWait() {
        return noWait;
    }

    public void setNoWait(boolean noWait) {
        this.noWait = noWait;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        reserved1 = Coder.readShort(in);
        queue = Coder.readShortString(in);
        ifUnused = Coder.readBit(in);
        ifEmpty = Coder.readBit(in);
        noWait = Coder.readBit(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeShort(reserved1, out);
        Coder.writeShortString(queue, out);
        Coder.writeBit(ifUnused, out);
        Coder.writeBit(ifEmpty, out);
        Coder.writeBit(noWait, out);
        out.bitFlush();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer(" ");
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("reserved1=");
        b.append(reserved1);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("queue=");
        b.append(queue);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("ifUnused=");
        b.append(ifUnused);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("ifEmpty=");
        b.append(ifEmpty);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("noWait=");
        b.append(noWait);
        return b.toString();
    }

    public String toString() {
        return "[Delete " + super.toString() + getDisplayString() + "]";
    }
}
