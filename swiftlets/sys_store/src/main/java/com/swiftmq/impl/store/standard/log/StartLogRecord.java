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

package com.swiftmq.impl.store.standard.log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StartLogRecord extends LogRecord {
    public StartLogRecord(long magic) {
        super(0, null, null, null, null, null);
        this.magic = magic;
    }

    public int getLogType() {
        return LogRecord.START;
    }

    public void writeContent(DataOutput out) throws IOException {
        out.writeLong(magic);
    }

    public void readContent(DataInput in) throws IOException {
        magic = in.readLong();
    }

    public void writeContent(DataOutput out, boolean includeMagic) throws IOException {
        out.writeLong(magic);
    }

    public void readContent(DataInput in, boolean includeMagic) throws IOException {
        magic = in.readLong();
    }

    public String toString() {
        return "[StartLogRecord, magic=" + magic + "]";
    }
}
