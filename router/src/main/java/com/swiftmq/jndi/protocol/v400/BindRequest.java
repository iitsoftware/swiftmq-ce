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

package com.swiftmq.jndi.protocol.v400;

import com.swiftmq.jms.DestinationFactory;
import com.swiftmq.jms.QueueImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BindRequest implements JNDIRequest {
    String name;
    QueueImpl queue;

    public BindRequest(String name, QueueImpl queue) {
        this.name = name;
        this.queue = queue;
    }

    public BindRequest() {
    }

    public int getDumpId() {
        return JNDIRequestFactory.BIND;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        out.writeUTF(name);
        DestinationFactory.dumpDestination(queue, out);
    }

    public void readContent(DataInput in)
            throws IOException {
        name = in.readUTF();
        queue = (QueueImpl) DestinationFactory.createDestination(in);
    }

    public String getName() {
        return (name);
    }

    public QueueImpl getQueue() {
        return (queue);
    }

    public void accept(JNDIRequestVisitor visitor) {
        visitor.visit(this);
    }

    public String toString() {
        return "[BindRequest, name=" + name + ", queue=" + queue + "]";
    }
}

