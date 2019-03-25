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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Compressor {
    public static byte[] compress(byte[] data) throws IOException {
        int len = data.length / 2;
        ByteArrayOutputStream aos = new ByteArrayOutputStream();
        GZIPOutputStream gos = new GZIPOutputStream(aos, len);
        BufferedOutputStream dos = new BufferedOutputStream(gos, len);
        dos.write(data, 0, data.length);
        dos.close();
        return aos.toByteArray();
    }

    public static byte[] decompress(byte[] data) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        GZIPInputStream gis = new GZIPInputStream(bis);
        BufferedInputStream dis = new BufferedInputStream(gis);
        ByteArrayOutputStream aos = new ByteArrayOutputStream();
        byte[] b = new byte[data.length];
        int c;
        while ((c = dis.read(b, 0, data.length)) != -1)
            aos.write(b, 0, c);
        dis.close();
        aos.close();
        return aos.toByteArray();
    }
}

