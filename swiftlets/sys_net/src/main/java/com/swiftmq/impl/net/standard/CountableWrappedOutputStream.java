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

import java.io.IOException;
import java.io.OutputStream;

public class CountableWrappedOutputStream extends OutputStream
        implements Countable {
    OutputStream out = null;
    volatile long byteCount = 0;

    public CountableWrappedOutputStream(OutputStream out) {
        this.out = out;
    }

    public void write(byte[] b, int offset, int len) throws IOException {
        byteCount += len;
        out.write(b, offset, len);
    }

    public void write(int b) throws IOException {
        byteCount++;
        out.write(b);
    }

    public void flush() throws IOException {
        out.flush();
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
}
