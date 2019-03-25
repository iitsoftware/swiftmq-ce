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

package com.swiftmq.impl.net.standard;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public abstract class CountableByteArrayOutputStream extends OutputStream
        implements Countable {
    volatile long byteCount = 0;
    protected ByteArrayOutputStream bos = null;

    public CountableByteArrayOutputStream() {
        bos = new ByteArrayOutputStream();
    }

    public CountableByteArrayOutputStream(int size) {
        bos = new ByteArrayOutputStream(size);
    }

    public void write(byte[] b, int offset, int len) throws IOException {
        byteCount += len;
        bos.write(b, offset, len);
    }

    public void write(int b) throws IOException {
        byteCount++;
        bos.write(b);
    }

    public void addByteCount(long cnt) {
        byteCount += cnt;
    }

    public long getByteCount() {
        return byteCount;
    }

    public void resetByteCount() {
        byteCount = 0;
    }

    public abstract void flush() throws IOException;
}

