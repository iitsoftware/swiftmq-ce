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

public class RecoverAsync extends BasicMethod {
    boolean requeue;

    public RecoverAsync() {
        _classId = 60;
        _methodId = 100;
    }

    public void accept(BasicMethodVisitor visitor) {
        visitor.visit(this);
    }

    public boolean getRequeue() {
        return requeue;
    }

    public void setRequeue(boolean requeue) {
        this.requeue = requeue;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        requeue = Coder.readBit(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeBit(requeue, out);
        out.bitFlush();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer(" ");
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("requeue=");
        b.append(requeue);
        return b.toString();
    }

    public String toString() {
        return "[RecoverAsync " + super.toString() + getDisplayString() + "]";
    }
}
