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

package com.swiftmq.amqp.v091.types;

import com.swiftmq.amqp.v091.io.BitSupportDataInput;
import com.swiftmq.amqp.v091.io.BitSupportDataInputStream;
import com.swiftmq.amqp.v091.io.BitSupportDataOutput;
import com.swiftmq.amqp.v091.io.BitSupportDataOutputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Method {
    protected int _classId = 0;
    protected int _methodId = 0;

    public int _getClassId() {
        return _classId;
    }

    public int _getMethodId() {
        return _methodId;
    }

    protected abstract void readBody(BitSupportDataInput in) throws IOException;

    protected abstract void writeBody(BitSupportDataOutput out) throws IOException;

    public void readContent(DataInput in) throws IOException {
        BitSupportDataInputStream bdis = new BitSupportDataInputStream(in);
        readBody(bdis);
    }

    public void writeContent(DataOutput out) throws IOException {
        BitSupportDataOutputStream bdos = new BitSupportDataOutputStream(out);
        Coder.writeShort(_classId, bdos);
        Coder.writeShort(_methodId, bdos);
        writeBody(bdos);
    }

    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("[Method");
        sb.append(" classId=").append(_classId);
        sb.append(", methodId=").append(_methodId);
        sb.append(']');
        return sb.toString();
    }
}
