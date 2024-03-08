
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

package com.swiftmq.impl.store.standard.index;

public class RootIndexEntry extends IndexEntry {
    byte[] queueNameByte = null;

    public void setKey(Comparable c) {
        queueNameByte = ((String) c).getBytes();
        super.setKey(c);
    }

    public int getLength() {
        if (key == null || rootPageNo == -1)
            throw new NullPointerException("key == null || rootPage == -1");
        return queueNameByte.length + 13;
    }

    public void writeContent(byte[] b, int offset) {
        int pos = offset;

        b[pos++] = (byte) (valid ? 1 : 0);

        // write the string length
        int len = queueNameByte.length;
        Util.writeInt(len, b, pos);
        pos += 4;

        // write the string
        for (int i = 0; i < len; i++)
            b[pos++] = queueNameByte[i];

        // write the rootPageNo
        Util.writeInt(rootPageNo, b, pos);
    }

    public void readContent(byte[] b, int offset) {
        int pos = offset;

        valid = b[pos++] == 1;

        // read the len
        int len = Util.readInt(b, pos);
        pos += 4;

        // read the string
        queueNameByte = new byte[len];
        System.arraycopy(b, pos, queueNameByte, 0, len);
        pos += len;
        key = new String(queueNameByte);

        // read the rootPage
        rootPageNo = Util.readInt(b, pos);
    }

    public String toString() {
        return "[RootIndexEntry, " + super.toString() + "]";
    }
}

