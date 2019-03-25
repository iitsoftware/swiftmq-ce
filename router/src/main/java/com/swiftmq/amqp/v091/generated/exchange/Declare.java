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

package com.swiftmq.amqp.v091.generated.exchange;

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

public class Declare extends ExchangeMethod {
    int reserved1;
    String exchange;
    String type;
    boolean passive;
    boolean durable;
    boolean autoDelete;
    boolean internal;
    boolean noWait;
    Map<String, Object> arguments;

    public Declare() {
        _classId = 40;
        _methodId = 10;
    }

    public void accept(ExchangeMethodVisitor visitor) {
        visitor.visit(this);
    }

    public int getReserved1() {
        return reserved1;
    }

    public void setReserved1(int reserved1) {
        this.reserved1 = reserved1;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public boolean getAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public boolean getInternal() {
        return internal;
    }

    public void setInternal(boolean internal) {
        this.internal = internal;
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
        exchange = Coder.readShortString(in);
        type = Coder.readShortString(in);
        passive = Coder.readBit(in);
        durable = Coder.readBit(in);
        autoDelete = Coder.readBit(in);
        internal = Coder.readBit(in);
        noWait = Coder.readBit(in);
        arguments = Coder.readTable(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeShort(reserved1, out);
        Coder.writeShortString(exchange, out);
        Coder.writeShortString(type, out);
        Coder.writeBit(passive, out);
        Coder.writeBit(durable, out);
        Coder.writeBit(autoDelete, out);
        Coder.writeBit(internal, out);
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
        b.append("exchange=");
        b.append(exchange);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("type=");
        b.append(type);
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
        b.append("autoDelete=");
        b.append(autoDelete);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("internal=");
        b.append(internal);
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
