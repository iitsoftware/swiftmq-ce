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
import java.util.Map;

public class Declare extends QueueMethod {
    int reserved1;
    String queue;
    boolean passive;
    boolean durable;
    boolean exclusive;
    boolean autoDelete;
    boolean noWait;
    Map<String, Object> arguments;

    public Declare() {
        _classId = 50;
        _methodId = 10;
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

    public boolean getPassive() {
        return passive;
    }

    public void setPassive(boolean passive) {
        this.passive = passive;
    }

    public boolean getDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean getExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public boolean getAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
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
        passive = Coder.readBit(in);
        durable = Coder.readBit(in);
        exclusive = Coder.readBit(in);
        autoDelete = Coder.readBit(in);
        noWait = Coder.readBit(in);
        arguments = Coder.readTable(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeShort(reserved1, out);
        Coder.writeShortString(queue, out);
        Coder.writeBit(passive, out);
        Coder.writeBit(durable, out);
        Coder.writeBit(exclusive, out);
        Coder.writeBit(autoDelete, out);
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
        b.append("passive=");
        b.append(passive);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("durable=");
        b.append(durable);
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
        b.append("autoDelete=");
        b.append(autoDelete);
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
        return "[Declare " + super.toString() + getDisplayString() + "]";
    }
}
