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

package com.swiftmq.amqp.v091.generated.connection;

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

public class Tune extends ConnectionMethod {
    int channelMax;
    int frameMax;
    int heartbeat;

    public Tune() {
        _classId = 10;
        _methodId = 30;
    }

    public void accept(ConnectionMethodVisitor visitor) {
        visitor.visit(this);
    }

    public int getChannelMax() {
        return channelMax;
    }

    public void setChannelMax(int channelMax) {
        this.channelMax = channelMax;
    }

    public int getFrameMax() {
        return frameMax;
    }

    public void setFrameMax(int frameMax) {
        this.frameMax = frameMax;
    }

    public int getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(int heartbeat) {
        this.heartbeat = heartbeat;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        channelMax = Coder.readShort(in);
        frameMax = Coder.readInt(in);
        heartbeat = Coder.readShort(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeShort(channelMax, out);
        Coder.writeInt(frameMax, out);
        Coder.writeShort(heartbeat, out);
        out.bitFlush();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer(" ");
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("channelMax=");
        b.append(channelMax);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("frameMax=");
        b.append(frameMax);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("heartbeat=");
        b.append(heartbeat);
        return b.toString();
    }

    public String toString() {
        return "[Tune " + super.toString() + getDisplayString() + "]";
    }
}
