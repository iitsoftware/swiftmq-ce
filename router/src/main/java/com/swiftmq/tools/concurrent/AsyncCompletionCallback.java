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

package com.swiftmq.tools.concurrent;

public abstract class AsyncCompletionCallback {
    volatile Object result = null;
    volatile Exception exception = null;
    protected volatile AsyncCompletionCallback next = null;
    volatile boolean notified = false;

    protected AsyncCompletionCallback() {
    }

    protected AsyncCompletionCallback(AsyncCompletionCallback next) {
        this.next = next;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public boolean isNotified() {
        return notified;
    }

    public void notifyCallbackStack(boolean success) {
        AsyncCompletionCallback current = this;
        while (current != null) {
            if (!current.notified) {
                current.done(success);
                current.notified = true;
            }
            current = current.next;
        }
    }

    public abstract void done(boolean success);

    public void reset() {
        result = null;
        exception = null;
        next = null;
        notified = false;
    }
}
