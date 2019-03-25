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

package com.swiftmq.amqp.v100.transport;

import com.swiftmq.amqp.v100.generated.transport.performatives.FrameVisitor;

import java.io.DataOutput;
import java.io.IOException;

/**
 * A HeartbeatFrame is a frame without a body and used for connection keepalive.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class HeartbeatFrame extends AMQPFrame {
    /**
     * Constructs a HeartbeatFrame
     *
     * @param channel channel
     */
    public HeartbeatFrame(int channel) {
        super(channel);
    }

    public void accept(FrameVisitor visitor) {
        visitor.visit(this);
    }

    protected void writeBody(DataOutput out) throws IOException {
    }

    public String toString() {
        return "[HeartbeatFrame]";
    }
}
