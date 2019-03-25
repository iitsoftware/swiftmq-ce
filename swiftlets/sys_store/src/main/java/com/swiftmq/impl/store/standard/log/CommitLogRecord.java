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

public class CommitLogRecord extends LogRecord {
    public CommitLogRecord(long txId, Semaphore semaphore, List journal, CacheReleaseListener cacheReleaseListener, AsyncCompletionCallback callback, List messagePageRefs) {
        super(txId, semaphore, journal, cacheReleaseListener, callback, messagePageRefs);
    }

    public CommitLogRecord(long txId, Semaphore semaphore, List journal, CacheReleaseListener cacheReleaseListener, List messagePageRefs) {
        super(txId, semaphore, journal, cacheReleaseListener, null, messagePageRefs);
    }

    public int getLogType() {
        return COMMIT;
    }

    public String toString() {
        return "[CommitLogRecord, magic=" + magic + ", txId=" + txId + ", Semaphore=" + semaphore + ", messagePageRefs=" + messagePageRefs + ", " + super.toString() + "]";
    }
}

