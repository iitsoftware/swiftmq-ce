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

package com.swiftmq.amqp.v091.generated.basic;

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
import java.util.Map;

public class Consume extends BasicMethod {
    int reserved1;
    String queue;
    String consumerTag;
    boolean noLocal;
    boolean noAck;
    boolean exclusive;
    boolean noWait;
    Map<String, Object> arguments;

    public Consume() {
        _classId = 60;
        _methodId = 20;
    }

    public void accept(BasicMethodVisitor visitor) {
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

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public boolean getNoLocal() {
        return noLocal;
    }

    public void setNoLocal(boolean noLocal) {
        this.noLocal = noLocal;
    }

    public boolean getNoAck() {
        return noAck;
    }

    public void setNoAck(boolean noAck) {
        this.noAck = noAck;
    }

    public boolean getExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public boolean getNoWait() {
        return noWait;
    }

    public void setNoWait(boolean noWait) {
        this.noWait = noWait;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        reserved1 = Coder.readShort(in);
        queue = Coder.readShortString(in);
        consumerTag = Coder.readShortString(in);
        noLocal = Coder.readBit(in);
        noAck = Coder.readBit(in);
        exclusive = Coder.readBit(in);
        noWait = Coder.readBit(in);
        arguments = Coder.readTable(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeShort(reserved1, out);
        Coder.writeShortString(queue, out);
        Coder.writeShortString(consumerTag, out);
        Coder.writeBit(noLocal, out);
        Coder.writeBit(noAck, out);
        Coder.writeBit(exclusive, out);
        Coder.writeBit(noWait, out);
        Coder.writeTable(arguments, out);
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
        b.append("consumerTag=");
        b.append(consumerTag);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("noLocal=");
        b.append(noLocal);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("noAck=");
        b.append(noAck);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("exclusive=");
        b.append(exclusive);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("noWait=");
        b.append(noWait);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("arguments=");
        b.append(arguments);
        return b.toString();
    }

    public String toString() {
        return "[Consume " + super.toString() + getDisplayString() + "]";
    }
}
