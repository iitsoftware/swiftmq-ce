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

public class Qos extends BasicMethod {
    int prefetchSize;
    int prefetchCount;
    boolean global;

    public Qos() {
        _classId = 60;
        _methodId = 10;
    }

    public void accept(BasicMethodVisitor visitor) {
        visitor.visit(this);
    }

    public int getPrefetchSize() {
        return prefetchSize;
    }

    public void setPrefetchSize(int prefetchSize) {
        this.prefetchSize = prefetchSize;
    }

    public int getPrefetchCount() {
        return prefetchCount;
    }

    public void setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
    }

    public boolean getGlobal() {
        return global;
    }

    public void setGlobal(boolean global) {
        this.global = global;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        prefetchSize = Coder.readInt(in);
        prefetchCount = Coder.readShort(in);
        global = Coder.readBit(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeInt(prefetchSize, out);
        Coder.writeShort(prefetchCount, out);
        Coder.writeBit(global, out);
        out.bitFlush();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer(" ");
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("prefetchSize=");
        b.append(prefetchSize);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("prefetchCount=");
        b.append(prefetchCount);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("global=");
        b.append(global);
        return b.toString();
    }

    public String toString() {
        return "[Qos " + super.toString() + getDisplayString() + "]";
    }
}
