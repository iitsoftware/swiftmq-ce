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

package com.swiftmq.jms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DestinationFactory {
    public static final byte TYPE_QUEUE = 0;
    public static final byte TYPE_TOPIC = 1;
    public static final byte TYPE_TEMPTOPIC = 2;
    public static final byte TYPE_TEMPQUEUE = 3;

    public static final DestinationImpl createDestination(DataInput in)
            throws IOException {
        byte b = in.readByte();
        DestinationImpl dest = null;
        switch (b) {
            case TYPE_TEMPQUEUE:
                dest = new TemporaryQueueImpl(null, null);
                break;
            case TYPE_QUEUE:
                dest = new QueueImpl();
                break;
            case TYPE_TOPIC:
                dest = new TopicImpl();
                break;
            case TYPE_TEMPTOPIC:
                dest = new TemporaryTopicImpl();
                break;
        }
        dest.readContent(in);
        return dest;
    }

    public static final void dumpDestination(DestinationImpl dest, DataOutput out)
            throws IOException {
        out.writeByte(dest.getType());
        dest.writeContent(out);
    }
}

