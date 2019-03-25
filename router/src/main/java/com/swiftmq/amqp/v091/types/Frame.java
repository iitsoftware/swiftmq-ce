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

package com.swiftmq.amqp.v091.types;

import com.swiftmq.amqp.Writable;
import com.swiftmq.amqp.v091.generated.MethodReader;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Frame implements Writable {
    public static final int TYPE_METHOD = 1;
    public static final int TYPE_HEADER = 2;
    public static final int TYPE_BODY = 3;
    public static final int TYPE_HEARTBEAT = 8;
    public static final int FRAME_END = 0xCE;

    int type = 0;
    int channel = 0;
    int size = 0;
    byte[] payload = null;
    Object payloadObject = null;
    int maxFrameSize = 0;
    Semaphore semaphore = null;
    AsyncCompletionCallback callback = null;

    public Frame(int type, int channel, int size, byte[] payload) {
        this.type = type;
        this.channel = channel;
        this.size = size;
        this.payload = payload;
    }

    public Frame(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public Frame generatePayloadObject() throws IOException {
        if (payloadObject != null || payload == null)
            return this;
        DataByteArrayInputStream dbis = new DataByteArrayInputStream();
        dbis.setBuffer(payload);
        switch (type) {
            case TYPE_METHOD:
                payloadObject = MethodReader.createMethod(dbis);
                break;
            case Frame.TYPE_HEADER:
                ContentHeaderProperties prop = new ContentHeaderProperties();
                prop.readContent(dbis);
                payloadObject = prop;
                break;
            default:
                break;
        }
        return this;
    }

    public Object getPayloadObject() throws IOException {
        if (payloadObject == null)
            generatePayloadObject();
        return payloadObject;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getChannel() {
        return channel;
    }

    public void setChannel(int channel) {
        this.channel = channel;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public void setSemaphore(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }

    public void setCallback(AsyncCompletionCallback callback) {
        this.callback = callback;
    }

    public AsyncCompletionCallback getCallback() {
        return callback;
    }

    public void readContent(DataInput in) throws IOException {
        type = Coder.readByte(in);
        channel = Coder.readShort(in);
        size = Coder.readInt(in);
        if (type != TYPE_HEARTBEAT) {
            if (size > 0) {
                if (size + 4 > maxFrameSize)
                    throw new IOException("frame size (" + (size + 4) + ") exceeds max frame size (" + maxFrameSize + ")");
                payload = Coder.readBytes(in, size);
            } else
                payload = null;
        }
        int fe = Coder.readUnsignedByte(in);
        if (fe != FRAME_END)
            throw new IOException("Invalid frame end detected: " + fe);
    }

    public void writeContent(DataOutput out) throws IOException {
        Coder.writeByte(type, out);
        Coder.writeShort(channel, out);
        Coder.writeInt(size, out);
        if (type != TYPE_HEARTBEAT) {
            if (payload != null)
                Coder.writeBytes(payload, 0, size, out);
        }
        Coder.writeByte(FRAME_END, out);
    }

    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("[Frame");
        sb.append(" type=").append(type);
        sb.append(", channel=").append(channel);
        if (type != TYPE_HEARTBEAT) {
            sb.append(", size=").append(size);
            if (type != TYPE_BODY)
                sb.append(", payloadObject=").append(payloadObject == null ? "null" : payloadObject);
        }
        sb.append(']');
        return sb.toString();
    }
}
