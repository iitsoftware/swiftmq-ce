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

package com.swiftmq.tools.requestreply;

import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.auth.ResourceLimitException;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueueLimitException;

import javax.jms.InvalidDestinationException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReplyNE extends Reply {
    public void writeContent(DataOutput out)
            throws IOException {
        out.writeBoolean(ok);
        if (exception == null)
            out.writeByte(0);
        else {
            out.writeByte(1);
            out.writeUTF(exception.getClass().getName());
            String s = exception.getMessage();
            if (s == null)
                out.writeByte(0);
            else {
                out.writeByte(1);
                out.writeUTF(s);
            }
        }
        out.writeBoolean(timeout);
        out.writeInt(requestNumber);
    }

    private void createException(String className, String msg) throws Exception {
        if (className.equals("com.swiftmq.swiftlet.queue.QueueException"))
            exception = new QueueException(msg);
        else if (className.equals("com.swiftmq.swiftlet.queue.QueueLimitException"))
            exception = new QueueLimitException(msg);
        else if (className.equals("com.swiftmq.swiftlet.queue.UnknownQueueException"))
            exception = new InvalidDestinationException(msg);
        else if (className.equals("com.swiftmq.swiftlet.auth.ResourceLimitException"))
            exception = new ResourceLimitException(msg);
        else if (className.equals("com.swiftmq.swiftlet.auth.AuthenticationException"))
            exception = new AuthenticationException(msg);
        else if (className.equals("javax.jms.InvalidDestinationException"))
            exception = new InvalidDestinationException(msg);
        else
            exception = new Exception(className + ": " + msg);
    }

    public void readContent(DataInput in)
            throws IOException {
        ok = in.readBoolean();
        byte set = in.readByte();
        if (set == 0)
            exception = null;
        else {
            String className = in.readUTF();
            String msg = null;
            set = in.readByte();
            if (set == 1)
                msg = in.readUTF();
            try {
                createException(className, msg);
            } catch (Exception e) {
                exception = new Exception("Unable to construct exception object of type '" + className + "'", e);
            }
        }
        timeout = in.readBoolean();
        requestNumber = in.readInt();
    }
}
