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

package com.swiftmq.tools.pipeline;

import com.swiftmq.tools.concurrent.Semaphore;

public abstract class POObject {
    POCallback callback = null;
    Semaphore semaphore = null;
    boolean success = false;
    String exception = null;

    public POObject(POCallback callback, Semaphore semaphore) {
        this.callback = callback;
        this.semaphore = semaphore;
    }

    public POCallback getCallback() {
        return callback;
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public abstract void accept(POVisitor visitor);
}
