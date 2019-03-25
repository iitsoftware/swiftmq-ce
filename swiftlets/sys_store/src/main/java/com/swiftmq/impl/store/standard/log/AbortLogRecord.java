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

package com.swiftmq.impl.store.standard.log;

import com.swiftmq.impl.store.standard.cache.CacheReleaseListener;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;

import java.util.List;

public class AbortLogRecord extends LogRecord {
    public AbortLogRecord(long txId, Semaphore semaphore, List journal, CacheReleaseListener cacheReleaseListener, AsyncCompletionCallback callback) {
        super(txId, semaphore, journal, cacheReleaseListener, callback, null);
    }

    public AbortLogRecord(long txId, Semaphore semaphore, List journal, CacheReleaseListener cacheReleaseListener) {
        super(txId, semaphore, journal, cacheReleaseListener, null, null);
    }

    public int getLogType() {
        return ABORT;
    }

    public String toString() {
        return "[AbortLogRecord, magic=" + magic + ", txId=" + txId + ", Semaphore=" + semaphore + ", " + super.toString() + "]";
    }
}

