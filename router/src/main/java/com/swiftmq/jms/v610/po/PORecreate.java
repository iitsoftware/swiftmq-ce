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

package com.swiftmq.jms.v610.po;

import com.swiftmq.jms.v610.Recreatable;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;
import com.swiftmq.tools.requestreply.Request;


public class PORecreate extends POObject {
    Recreatable recreatable = null;
    Request request = null;

    public PORecreate(Semaphore semaphore, Recreatable recreatable, Request request) {
        super(null, semaphore);
        this.recreatable = recreatable;
        this.request = request;
    }

    public Recreatable getRecreatable() {
        return recreatable;
    }

    public Request getRequest() {
        return request;
    }

    public void accept(POVisitor visitor) {
        ((ReconnectVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[PORecreate, recreatable=" + recreatable + ", request=" + request + "]";
    }
}
