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

public class Deliver extends BasicMethod {
    String consumerTag;
    long deliveryTag;
    boolean redelivered;
    String exchange;
    String routingKey;

    public Deliver() {
        _classId = 60;
        _methodId = 60;
    }

    public void accept(BasicMethodVisitor visitor) {
        visitor.visit(this);
    }

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public long getDeliveryTag() {
        return deliveryTag;
    }

    public void setDeliveryTag(long deliveryTag) {
        this.deliveryTag = deliveryTag;
    }

    public boolean getRedelivered() {
        return redelivered;
    }

    public void setRedelivered(boolean redelivered) {
        this.redelivered = redelivered;
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

    protected void readBody(BitSupportDataInput in) throws IOException {
        consumerTag = Coder.readShortString(in);
        deliveryTag = Coder.readLong(in);
        redelivered = Coder.readBit(in);
        exchange = Coder.readShortString(in);
        routingKey = Coder.readShortString(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeShortString(consumerTag, out);
        Coder.writeLong(deliveryTag, out);
        Coder.writeBit(redelivered, out);
        Coder.writeShortString(exchange, out);
        Coder.writeShortString(routingKey, out);
        out.bitFlush();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer(" ");
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
        b.append("deliveryTag=");
        b.append(deliveryTag);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("redelivered=");
        b.append(redelivered);
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
        return b.toString();
    }

    public String toString() {
        return "[Deliver " + super.toString() + getDisplayString() + "]";
    }
}
