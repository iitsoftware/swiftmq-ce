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

package com.swiftmq.tools.util;

import java.io.*;

public class LazyUTF8String implements Serializable {
    String s = null;
    byte[] buffer = null;
    int utfLength = 0;

    public LazyUTF8String(DataInput in) throws IOException {
        utfLength = in.readUnsignedShort();
        buffer = new byte[utfLength + 2];
        in.readFully(buffer, 2, utfLength);
        buffer[0] = (byte) ((utfLength >>> 8) & 0xFF);
        buffer[1] = (byte) ((utfLength) & 0xFF);
    }

    public LazyUTF8String(String s) {
        try {
            if (s == null) {
                System.out.println("s==null");
                throw new NullPointerException();

            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            throw e;
        }
        this.s = s;
    }

    private String bufferToString() throws Exception {
        return UTFUtils.convertFromUTF8(buffer, 2, utfLength);
    }

    private byte[] stringToBuffer() throws Exception {
        utfLength = UTFUtils.countUTFBytes(s);
        if (utfLength > 65535)
            throw new UTFDataFormatException();

        byte[] b = new byte[utfLength + 2];
        int count = 0;
        count = UTFUtils.writeShortToBuffer(utfLength, b, count);
        UTFUtils.writeUTFBytesToBuffer(s, b, count);
        return b;
    }

    public String getString() {
        return getString(false);
    }

    public String getString(boolean clear) {
        if (s == null) {
            synchronized (this) {
                if (s == null) {
                    try {
                        s = bufferToString();
                        if (clear) {
                            buffer = null;
                            utfLength = 0;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return s;
    }

    public byte[] getBuffer() {
        if (buffer == null) {
            synchronized (this) {
                if (buffer == null) {
                    try {
                        buffer = stringToBuffer();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return buffer;
    }

    public void writeContent(DataOutput out) throws IOException {
        out.write(getBuffer());
    }

    public String toString() {
        return "[LazyUTF8String, s=" + s + ", buffer=" + buffer + "]";
    }

}
