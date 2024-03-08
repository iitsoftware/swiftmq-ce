/*
 * Copyright 2024 IIT Software GmbH
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

package com.swiftmq.impl.queue.standard.queue;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.Selector;

import java.util.SortedSet;
import java.util.TreeSet;

public class View {
    SwiftletContext ctx = null;
    String queueName = null;
    int viewId = -1;
    Selector selector = null;
    SortedSet<StoreId> viewContent = new TreeSet<StoreId>();
    boolean dirty = false;

    public View(SwiftletContext ctx, String queueName, int viewId, Selector selector) {
        this.ctx = ctx;
        this.queueName = queueName;
        this.viewId = viewId;
        this.selector = selector;
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(queueName, toString() + "/created");
    }

    public int getViewId() {
        return viewId;
    }

    public void setViewId(int viewId) {
        this.viewId = viewId;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public void storeOnMatch(StoreId storeId, MessageImpl message) {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(queueName, toString() + "/storeOnMatch, storeId=" + storeId + ", message=" + message);
        if (selector.isSelected(message)) {
            viewContent.add(storeId);
            dirty = true;
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(queueName, toString() + "/storeOnMatch, match, added");
        } else {
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(queueName, toString() + "/storeOnMatch, no match");
        }
    }

    public void remove(StoreId storeId) {
        viewContent.remove(storeId);
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(queueName, toString() + "/remove, storeId=" + storeId);
    }

    public SortedSet<StoreId> getViewContent() {
        return viewContent;
    }

    public void close() {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(queueName, toString() + "/close");
        selector = null;
        viewContent = null;
    }

    public String toString() {
        return "view, id=" + viewId + ", selector=" + selector.getConditionString() + ", size=" + viewContent.size() + ", dirty=" + dirty;
    }
}
