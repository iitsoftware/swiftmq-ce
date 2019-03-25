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

package com.swiftmq.impl.queue.standard.accounting;

import com.swiftmq.impl.queue.standard.SwiftletContext;

import java.util.regex.Pattern;

public class AccountingProfile {
    SwiftletContext ctx = null;
    String tracePrefix = "AccountingProfile";
    QueueManagerSource source = null;
    Pattern queueNameFilter = null;
    boolean queueNameFilterNegate = false;

    public AccountingProfile(SwiftletContext ctx, Pattern queueNameFilter, boolean queueNameFilterNegate) {
        this.ctx = ctx;
        this.queueNameFilter = queueNameFilter;
        this.queueNameFilterNegate = queueNameFilterNegate;
    }

    public void setSource(QueueManagerSource source) {
        this.source = source;
    }

    public QueueManagerSource getSource() {
        return source;
    }

    private boolean checkMatch(Pattern p, boolean negate, String value) {
        if (value == null && p != null)
            return false;
        if (p == null)
            return true;
        boolean b = p.matcher(value).matches();
        return negate ? !b : b;
    }

    private boolean dbg(String s, boolean b) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), tracePrefix + "/" + s + ": " + b);
        return b;
    }

    public boolean isMatchQueueName(String queueName) {
        return dbg("isMatchQueueName", checkMatch(queueNameFilter, queueNameFilterNegate, queueName));
    }

    public String toString() {
        return "[AccountingProfile, source=" + source + ", queueNameFilter=" + queueNameFilter + ", queueNameFilterNegate=" + queueNameFilterNegate + "]";
    }
}
