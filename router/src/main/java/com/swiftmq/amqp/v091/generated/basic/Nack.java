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

public class Nack extends BasicMethod {
    long deliveryTag;
    boolean multiple;
    boolean requeue;

    public Nack() {
        _classId = 60;
        _methodId = 120;
    }

    public void accept(BasicMethodVisitor visitor) {
        visitor.visit(this);
    }

    public long getDeliveryTag() {
        return deliveryTag;
    }

    public void setDeliveryTag(long deliveryTag) {
        this.deliveryTag = deliveryTag;
    }

    public boolean getMultiple() {
        return multiple;
    }

    public void setMultiple(boolean multiple) {
        this.multiple = multiple;
    }

    public boolean getRequeue() {
        return requeue;
    }

    public void setRequeue(boolean requeue) {
        this.requeue = requeue;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        deliveryTag = Coder.readLong(in);
        multiple = Coder.readBit(in);
        requeue = Coder.readBit(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeLong(deliveryTag, out);
        Coder.writeBit(multiple, out);
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
        b.append("deliveryTag=");
        b.append(deliveryTag);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("multiple=");
        b.append(multiple);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("requeue=");
        b.append(requeue);
        return b.toString();
    }

    public String toString() {
        return "[Nack " + super.toString() + getDisplayString() + "]";
    }
}
