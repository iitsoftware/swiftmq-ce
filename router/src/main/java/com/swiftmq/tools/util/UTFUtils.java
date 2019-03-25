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

import java.io.DataInput;
import java.io.IOException;
import java.io.UTFDataFormatException;

public class UTFUtils {
    public static String decodeUTF(int utfSize, DataInput in) throws IOException {
        byte[] buf = new byte[utfSize];
        char[] out = new char[utfSize];
        in.readFully(buf, 0, utfSize);

        return convertUTF8WithBuf(buf, out, 0, utfSize);
    }

    public static String convertFromUTF8(byte[] buf, int offset, int utfSize)
            throws UTFDataFormatException {
        return convertUTF8WithBuf(buf, new char[utfSize], offset, utfSize);
    }

    public static String convertUTF8WithBuf(byte[] buf, char[] out, int offset,
                                            int utfSize) throws UTFDataFormatException {
        int count = 0, s = 0, a;
        while (count < utfSize) {
            if ((out[s] = (char) buf[offset + count++]) < '\u0080')
                s++;
            else if (((a = out[s]) & 0xe0) == 0xc0) {
                if (count >= utfSize)
                    throw new UTFDataFormatException();
                int b = buf[offset + count++];
                if ((b & 0xC0) != 0x80)
                    throw new UTFDataFormatException();
                out[s++] = (char) (((a & 0x1F) << 6) | (b & 0x3F));
            } else if ((a & 0xf0) == 0xe0) {
                if (count + 1 >= utfSize)
                    throw new UTFDataFormatException();
                int b = buf[offset + count++];
                int c = buf[offset + count++];
                if (((b & 0xC0) != 0x80) || ((c & 0xC0) != 0x80))
                    throw new UTFDataFormatException();
                out[s++] = (char) (((a & 0x0F) << 12) | ((b & 0x3F) << 6) | (c & 0x3F));
            } else {
                throw new UTFDataFormatException();
            }
        }
        return new String(out, 0, s);
    }

    public static int countUTFBytes(String str) {
        int utfCount = 0, length = str.length();
        for (int i = 0; i < length; i++) {
            int charValue = str.charAt(i);
            if (charValue > 0 && charValue <= 127) {
                utfCount++;
            } else if (charValue <= 2047) {
                utfCount += 2;
            } else {
                utfCount += 3;
            }
        }
        return utfCount;
    }

    public static int writeShortToBuffer(int val, byte[] buffer, int offset) throws IOException {
        buffer[offset++] = (byte) (val >> 8);
        buffer[offset++] = (byte) val;
        return offset;
    }

    public static int writeUTFBytesToBuffer(String str, byte[] buffer, int offset) throws IOException {
        int length = str.length();
        for (int i = 0; i < length; i++) {
            int charValue = str.charAt(i);
            if (charValue > 0 && charValue <= 127) {
                buffer[offset++] = (byte) charValue;
            } else if (charValue <= 2047) {
                buffer[offset++] = (byte) (0xc0 | (0x1f & (charValue >> 6)));
                buffer[offset++] = (byte) (0x80 | (0x3f & charValue));
            } else {
                buffer[offset++] = (byte) (0xe0 | (0x0f & (charValue >> 12)));
                buffer[offset++] = (byte) (0x80 | (0x3f & (charValue >> 6)));
                buffer[offset++] = (byte) (0x80 | (0x3f & charValue));
            }
        }
        return offset;
    }
}