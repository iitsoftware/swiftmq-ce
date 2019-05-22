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

package com.swiftmq.extension.amqpbridge.v100;

import com.swiftmq.amqp.integration.Tracer;
import com.swiftmq.swiftlet.trace.TraceSpace;

public class RouterTracer extends Tracer {
    TraceSpace traceSpace = null;

    public RouterTracer(TraceSpace traceSpace) {
        this.traceSpace = traceSpace;
    }

    public boolean isEnabled() {
        return traceSpace.enabled;
    }

    public void trace(String key, String msg) {
        traceSpace.trace(key, msg);
    }
}
