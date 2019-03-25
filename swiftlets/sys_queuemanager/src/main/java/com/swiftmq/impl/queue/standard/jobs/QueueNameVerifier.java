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

package com.swiftmq.impl.queue.standard.jobs;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.swiftlet.scheduler.InvalidValueException;
import com.swiftmq.swiftlet.scheduler.JobParameter;
import com.swiftmq.swiftlet.scheduler.JobParameterVerifier;

public class QueueNameVerifier implements JobParameterVerifier {
    SwiftletContext ctx = null;

    public QueueNameVerifier(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public void verify(JobParameter jobParameter, String value) throws InvalidValueException {
        if (value == null || value.trim().length() == 0)
            throw new InvalidValueException(jobParameter.getName() + " is mandatory!");
        if (!ctx.queueManager.isQueueDefined(value))
            throw new InvalidValueException("Queue '" + value + "' is unknown!");
    }
}
