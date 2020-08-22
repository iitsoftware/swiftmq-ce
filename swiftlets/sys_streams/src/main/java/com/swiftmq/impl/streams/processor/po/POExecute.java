/*
 * Copyright 2020 IIT Software GmbH
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

package com.swiftmq.impl.streams.processor.po;

import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POExecute extends POObject {
    Runnable runnable;

    public POExecute(Semaphore semaphore, Runnable runnable) {
        super(null, semaphore);
        this.runnable = runnable;
    }

    public Runnable getRunnable() {
        return runnable;
    }

    public void accept(POVisitor poVisitor) {
        ((POStreamVisitor) poVisitor).visit(this);
    }

    public String toString() {
        return "[POExecute, runnable=" + runnable + "]";
    }
}
