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
import java.util.concurrent.atomic.AtomicLong;

public class CountableInput extends InputStream implements Countable {
    final AtomicLong count = new AtomicLong();

    @Override
    public int read() throws IOException {
        return 0;
    }

    @Override
    public void addByteCount(long cnt) {
        count.addAndGet(cnt);
    }

    @Override
    public long getByteCount() {
        return count.get();
    }

    @Override
    public void resetByteCount() {
        count.set(0);
    }
}
