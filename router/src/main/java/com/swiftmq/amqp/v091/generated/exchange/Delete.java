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

public class Delete extends ExchangeMethod {
    int reserved1;
    String exchange;
    boolean ifUnused;
    boolean noWait;

    public Delete() {
        _classId = 40;
        _methodId = 20;
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

    public boolean getIfUnused() {
        return ifUnused;
    }

    public void setIfUnused(boolean ifUnused) {
        this.ifUnused = ifUnused;
    }

    public boolean getNoWait() {
        return noWait;
    }

    public void setNoWait(boolean noWait) {
        this.noWait = noWait;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        reserved1 = Coder.readShort(in);
        exchange = Coder.readShortString(in);
        ifUnused = Coder.readBit(in);
        noWait = Coder.readBit(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeShort(reserved1, out);
        Coder.writeShortString(exchange, out);
        Coder.writeBit(ifUnused, out);
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
        b.append("exchange=");
        b.append(exchange);
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
        b.append("noWait=");
        b.append(noWait);
        return b.toString();
    }

    public String toString() {
        return "[Delete " + super.toString() + getDisplayString() + "]";
    }
}
