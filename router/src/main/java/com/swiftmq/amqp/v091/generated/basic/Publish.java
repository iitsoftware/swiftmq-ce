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

public class Publish extends BasicMethod {
    int reserved1;
    String exchange;
    String routingKey;
    boolean mandatory;
    boolean immediate;

    public Publish() {
        _classId = 60;
        _methodId = 40;
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

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public boolean getMandatory() {
        return mandatory;
    }

    public void setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
    }

    public boolean getImmediate() {
        return immediate;
    }

    public void setImmediate(boolean immediate) {
        this.immediate = immediate;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        reserved1 = Coder.readShort(in);
        exchange = Coder.readShortString(in);
        routingKey = Coder.readShortString(in);
        mandatory = Coder.readBit(in);
        immediate = Coder.readBit(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeShort(reserved1, out);
        Coder.writeShortString(exchange, out);
        Coder.writeShortString(routingKey, out);
        Coder.writeBit(mandatory, out);
        Coder.writeBit(immediate, out);
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
        b.append("routingKey=");
        b.append(routingKey);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("mandatory=");
        b.append(mandatory);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("immediate=");
        b.append(immediate);
        return b.toString();
    }

    public String toString() {
        return "[Publish " + super.toString() + getDisplayString() + "]";
    }
}
