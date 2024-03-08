/*
 * Copyright 2023 IIT Software GmbH
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

package com.swiftmq.impl.store.standard.processor.backup.po;

import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class ScanSaveSets extends POObject {
    POObject nextPO = null;

    public ScanSaveSets() {
        super(null, null);
    }

    public ScanSaveSets(Semaphore semaphore) {
        super(null, semaphore);
    }

    public ScanSaveSets(POObject nextPO) {
        super(null, null);
        this.nextPO = nextPO;
    }

    public POObject getNextPO() {
        return nextPO;
    }

    public void accept(POVisitor poVisitor) {
        ((EventVisitor) poVisitor).visit(this);
    }

    public String toString() {
        return "[ScanSaveSets, nextPO=" + nextPO + "]";
    }
}
