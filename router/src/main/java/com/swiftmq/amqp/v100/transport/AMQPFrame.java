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

package com.swiftmq.amqp.v100.transport;

import com.swiftmq.amqp.Writable;
import com.swiftmq.amqp.v100.generated.security.sasl.SaslFrameIF;
import com.swiftmq.amqp.v100.generated.security.sasl.SaslFrameVisitor;
import com.swiftmq.amqp.v100.generated.transport.performatives.FrameIF;
import com.swiftmq.amqp.v100.generated.transport.performatives.FrameVisitor;
import com.swiftmq.amqp.v100.types.Util;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for all AMQP and SASL frames.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public abstract class AMQPFrame implements Writable, FrameIF, SaslFrameIF {
    public static byte TYPE_CODE_AMQP_FRAME = 0x00;
    public static byte TYPE_CODE_SASL_FRAME = 0x01;
    protected static int HEADER_SIZE = 8;

    long frameSize = HEADER_SIZE;
    byte dataOffset = 2;
    byte typeCode = TYPE_CODE_AMQP_FRAME;
    int channel = 0;
    byte[] header = new byte[HEADER_SIZE];
    byte[] payload = null;
    List morePayloads = null;

    DataByteArrayOutputStream dos = null;
    volatile Semaphore semaphore = null;
    volatile AsyncCompletionCallback callback = null;

    protected AMQPFrame(int channel) {
        this.channel = channel;
    }

    /**
     * Return the channel.
     *
     * @return channel
     */
    public int getChannel() {
        return channel;
    }

    /**
     * Sets the channel.
     *
     * @param channel chanmel
     */
    public void setChannel(int channel) {
        this.channel = channel;
    }

    protected void setTypeCode(byte typeCode) {
        this.typeCode = typeCode;
    }

    public void setSemaphore(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }

    public AsyncCompletionCallback getCallback() {
        return callback;
    }

    public void setCallback(AsyncCompletionCallback callback) {
        this.callback = callback;
    }

    public int getPredictedSize() {
        int size = HEADER_SIZE;
        if (payload != null)
            size += payload.length;
        return size;
    }

    /**
     * Returns the payload
     *
     * @return payload
     */
    public byte[] getPayload() {
        return payload;
    }

    /**
     * Sets the payload
     *
     * @param payload payload
     */
    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    /**
     * Adds more payload. This is used at the first frame on receive if a message is split over multiple transfer frames.
     *
     * @param payload payload
     */
    public void addMorePayload(byte[] payload) {
        if (morePayloads == null)
            morePayloads = new ArrayList();
        morePayloads.add(payload);
    }

    /**
     * Returns the list of additional payload (excluding the "main" payload).
     *
     * @return additional payload
     */
    public List getMorePayloads() {
        return morePayloads;
    }

    /**
     * Returns the length of the complete payload (main plus additional payload)
     *
     * @return payload length
     */
    public int getPayloadLength() {
        int n = payload.length;
        if (morePayloads != null) {
            for (int i = 0; i < morePayloads.size(); i++)
                n += ((byte[]) morePayloads.get(i)).length;
        }
        return n;
    }

    public void accept(FrameVisitor visitor) {
    }

    public void accept(SaslFrameVisitor visitor) {
    }

    protected abstract void writeBody(DataOutput out) throws IOException;

    public void writeContent(DataOutput out) throws IOException {
        if (dos == null)
            dos = new DataByteArrayOutputStream();
        dos.rewind();
        writeBody(dos);
        frameSize = HEADER_SIZE + dos.getCount() + (payload != null ? payload.length : 0);
        Util.writeInt((int) frameSize, header, 0);
        header[4] = dataOffset;
        header[5] = typeCode;
        Util.writeShort(channel, header, 6);
        out.write(header);
        if (dos.getCount() > 0)
            out.write(dos.getBuffer(), 0, dos.getCount());
        if (payload != null)
            out.write(payload, 0, payload.length);
    }

    public String getValueString() {
        return null;
    }

    public String toString() {
        return "[AMQPFrame, frameSize=" + frameSize + ", dataOffset=" + dataOffset + ", typeCode=" + typeCode + ", channel=" + channel + ", payload=" + (payload != null ? (payload.length + " bytes") : "null") + "]";
    }
}
