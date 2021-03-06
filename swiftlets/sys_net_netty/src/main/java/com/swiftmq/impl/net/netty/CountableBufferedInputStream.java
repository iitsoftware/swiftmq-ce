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

package com.swiftmq.impl.net.netty;

import java.io.IOException;
import java.io.InputStream;

public class CountableBufferedInputStream extends InputStream
        implements Countable {
    InputStream in = null;
    volatile long byteCount = 0;

    public CountableBufferedInputStream(InputStream in) {
        this.in = in;
    }

    public int read() throws IOException {
        byteCount++;
        return in.read();
    }

    public int read(byte[] b, int offset, int len) throws IOException {
        int rc = in.read(b, offset, len);
        if (rc != -1)
            byteCount += rc;
        return rc;
    }

    public int available() throws IOException {
        return in.available();
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

