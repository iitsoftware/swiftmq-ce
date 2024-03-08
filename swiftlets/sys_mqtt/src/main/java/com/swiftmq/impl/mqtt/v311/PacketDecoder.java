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

package com.swiftmq.impl.mqtt.v311;

import com.swiftmq.impl.mqtt.v311.netty.buffer.ByteBuf;
import com.swiftmq.impl.mqtt.v311.netty.handler.codec.mqtt.MqttDecoder;
import com.swiftmq.impl.mqtt.v311.netty.handler.codec.mqtt.MqttMessage;
import com.swiftmq.impl.mqtt.v311.netty.handler.codec.mqtt.MqttMessageType;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.InboundHandler;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.tools.util.DataStreamInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PacketDecoder implements InboundHandler {

    private static final int INITIAL_SIZE = 2048;
    private static final int EXTEND_SIZE = 1024;
    private static final int MAX_FHEADERS_SIZE = 5;

    MqttListener mqttListener;
    TraceSpace traceSpace;
    int maxPacketSize;
    MqttDecoder mqttDecoder;
    byte[] buffer = new byte[INITIAL_SIZE];
    final AtomicInteger writePos = new AtomicInteger();
    final AtomicInteger readPos = new AtomicInteger();
    final AtomicInteger remainingLength = new AtomicInteger(-1);
    final AtomicInteger packetSize = new AtomicInteger(1);
    final AtomicInteger maxFixedHeaderSize = new AtomicInteger(-1);
    DataStreamInputStream dis = new DataStreamInputStream();
    List<MqttMessage> decoded = new ArrayList<MqttMessage>();


    public PacketDecoder(MqttListener mqttListener, TraceSpace traceSpace, int maxPacketSize) {
        this.mqttListener = mqttListener;
        this.traceSpace = traceSpace;
        this.maxPacketSize = maxPacketSize;
        this.mqttDecoder = new MqttDecoder(maxPacketSize);
    }

    public void ensureBuffer(int ensureSize) {
        if (buffer.length - writePos.get() < ensureSize) {
            byte[] b = new byte[buffer.length + Math.max(ensureSize, EXTEND_SIZE)];
            System.arraycopy(buffer, 0, b, 0, writePos.get());
            buffer = b;
            if (traceSpace.enabled)
                trace("PacketDecoder/ensureBuffer: ensureSize=" + ensureSize + ", buffer.length=" + buffer.length);
        }
    }

    private int getFixedHeaderSize(int messageType) {
        int size;
        MqttMessageType type = MqttMessageType.valueOf(messageType);
        switch (type) {
            case PINGREQ:
            case PINGRESP:
            case DISCONNECT:
            case PUBACK:
            case PUBCOMP:
            case PUBREL:
            case PUBREC:
            case UNSUBACK:
                size = 2;
                break;
            default:
                size = MAX_FHEADERS_SIZE;
        }
        if (traceSpace.enabled)
            trace("PacketDecoder/getFixedHeaderSize: messageType=" + messageType);
        return size;
    }

    private void decodeFixedHeader() throws Exception {
        remainingLength.set(0);
        int multiplier = 1;
        short digit;
        int loops = 0;
        int bpos = readPos.get() + 1;
        do {
            digit = (short) (buffer[bpos++] & 0xff);
            remainingLength.addAndGet((digit & 127) * multiplier);
            multiplier *= 128;
            loops++;
            packetSize.getAndIncrement();
        } while ((digit & 128) != 0 && loops < 4);

        // MQTT protocol limits Remaining Length to 4 bytes
        if (loops == 4 && (digit & 128) != 0) {
            throw new Exception("remaining length exceeds 4 digits");
        }
        packetSize.addAndGet(remainingLength.get());
        if (traceSpace.enabled)
            trace("PacketDecoder/decodeFixedHeader");
    }

    private void reset() {
        writePos.set(0);
        readPos.set(0);
        remainingLength.set(-1);
        packetSize.set(1);
        maxFixedHeaderSize.set(-1);
        if (traceSpace.enabled)
            trace("PacketDecoder/reset");
    }

    private void nextPacket() {
        remainingLength.set(-1);
        packetSize.set(1);
        maxFixedHeaderSize.set(-1);
        if (traceSpace.enabled)
            trace("PacketDecoder/nextPacket");
    }

    private int available() {
        if (traceSpace.enabled)
            trace("available=" + (writePos.get() - readPos.get()));
        return writePos.get() - readPos.get();
    }

    private void packetCompleted(Connection connection) throws IOException {
        if (traceSpace.enabled)
            trace("PacketDecoder/packetCompleted");
        if (available() > 0) {
            nextPacket();
        } else
            reset();
    }

    private void finishPacket(Connection connection) throws IOException {
        if (traceSpace.enabled)
            trace("PacketDecoder/finishPacket");
        if (available() >= packetSize.get()) {
            ByteBuf byteBuf = new ByteBuf(packetSize.get());
            byteBuf.writeBytes(buffer, readPos.get(), packetSize.get());
            byteBuf.reset();
            try {
                mqttDecoder.decode(byteBuf, decoded);
                mqttListener.onMessage(decoded);
                decoded.clear();
            } catch (Exception e) {
                if (traceSpace.enabled)
                    trace("PacketDecoder/finishPacket: exception=" + e);
                mqttListener.onException(e);
            }
            readPos.addAndGet(packetSize.get());
            packetCompleted(connection);
        }
    }

    private void trace(String func) {
        traceSpace.trace("mqtt", func + ": readPos=" + readPos.get() + ", writePos=" + writePos.get() + ", packetSize=" + packetSize.get() + ", maxFixedHeaderSize=" + maxFixedHeaderSize.get() + ", remainingLength=" + remainingLength.get());
    }

    @Override
    public void dataAvailable(Connection connection, InputStream inputStream) throws IOException {
        dis.setInputStream(inputStream);
        int length = dis.available();
        if (traceSpace.enabled)
            trace("PacketDecoder/dataAvailable, length=" + length);
        ensureBuffer(length);
        dis.readFully(buffer, writePos.get(), length);
        writePos.addAndGet(length);
        int avail;
        do {
            if (remainingLength.get() == -1) {
                if (maxFixedHeaderSize.get() == -1)
                    maxFixedHeaderSize.set(getFixedHeaderSize((buffer[readPos.get()] & 0xff) >> 4));
                if (maxFixedHeaderSize.get() != -1 && available() >= maxFixedHeaderSize.get()) {
                    try {
                        decodeFixedHeader();
                        if (packetSize.get() > maxPacketSize)
                            throw new Exception("packet size of " + packetSize.get() + " bytes exceeds max packet size of " + maxPacketSize + " bytes");
                        finishPacket(connection);
                    } catch (Exception e) {
                        if (traceSpace.enabled)
                            trace("PacketDecoder/dataAvailable (1)");
                        mqttListener.onException(e);
                    }
                } else
                    break; // Need more data
            } else
                finishPacket(connection);
            avail = available();
        } while (avail > 0 && avail > packetSize.get());
        if (available() == 0)
            reset();
    }

}
