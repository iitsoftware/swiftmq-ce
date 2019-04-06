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
package com.swiftmq.impl.mqtt.v311.netty.buffer;


import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

public class ByteBuf {
    private static final int EXTEND_SIZE = 1024;
    byte[] buffer = null;
    int pos = 0;

    public ByteBuf(int len) {
        buffer = new byte[len];
    }

    public ByteBuf(byte[] buffer) {
        this.buffer = buffer;
    }

    public ByteBuf(byte[] b, int index, int length) {
        this.buffer = new byte[length];
        System.arraycopy(b, index, this.buffer, 0, length);
    }

    private void ensureBuffer(int ensureSize) {
        if (buffer.length - pos < ensureSize) {
            byte[] b = new byte[buffer.length + Math.max(ensureSize, EXTEND_SIZE)];
            System.arraycopy(buffer, 0, b, 0, pos);
            buffer = b;
        }
    }

    public ByteBuf duplicate() {
        byte[] b = new byte[buffer.length];
        System.arraycopy(buffer, 0, b, 0, buffer.length);
        return new ByteBuf(b);
    }

    public int readableBytes() {
        return buffer.length - pos;
    }

    public int size() {
        return buffer.length;
    }

    public void reset() {
        pos = 0;
    }

    public short readUnsignedByte() {
        return (short) (buffer[pos++] & 0xff);
    }

    public byte readByte() {
        return buffer[pos++];
    }

    public void readBytes(byte[] b) {
        System.arraycopy(buffer, pos, b, 0, b.length);
        pos += b.length;
    }

    public ByteBuf writeByte(int value) {
        ensureBuffer(1);
        ByteBufUtil.setByte(buffer, pos++, value);
        return this;
    }

    public ByteBuf writeShort(int value) {
        ensureBuffer(2);
        ByteBufUtil.setShort(buffer, pos, value);
        pos += 2;
        return this;
    }

    public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
        ensureBuffer(length);
        System.arraycopy(src, srcIndex, buffer, pos, length);
        pos += length;
        return this;
    }

    public ByteBuf writeBytes(byte[] src) {
        writeBytes(src, 0, src.length);
        return this;
    }

    public ByteBuf writeBytes(ByteBuf src) {
        writeBytes(src.buffer, src.pos, src.readableBytes());
        return this;
    }

    public void flushTo(OutputStream out) throws IOException {
        int len = pos;
        out.write(buffer, 0, len);
    }

    public void skipBytes(int n) {
        pos += n;
    }

    public int readerIndex() {
        return pos;
    }

    public ByteBuf readRetainedSlice(int length) {
        ByteBuf slice = new ByteBuf(buffer, pos, length);
        pos += length;
        return slice;
    }

    private static void decodeString(CharsetDecoder decoder, ByteBuffer src, CharBuffer dst) {
        try {
            CoderResult cr = decoder.decode(src, dst, true);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
            cr = decoder.flush(dst);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
        } catch (CharacterCodingException x) {
            throw new IllegalStateException(x);
        }
    }

    public String toString(int index, int len, Charset charset) {
        if (len == 0) {
            return "";
        }
        final CharsetDecoder decoder = charset.newDecoder();
        final int maxLength = (int) ((double) len * decoder.maxCharsPerByte());
        CharBuffer dst = CharBuffer.allocate(maxLength);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, pos, len);
        decodeString(decoder, byteBuffer, dst);
        return dst.flip().toString();
    }
}
