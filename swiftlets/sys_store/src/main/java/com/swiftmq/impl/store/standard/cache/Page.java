
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

package com.swiftmq.impl.store.standard.cache;

import com.swiftmq.impl.store.standard.pagedb.PageSize;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Page {
    public final static int HEADER_LENGTH = 1;
    public int pageNo = -1;
    public boolean dirty = false;
    public boolean empty = true;
    public byte[] data = null;

    public Page copy() {
        Page p = new Page();
        p.pageNo = pageNo;
        p.dirty = dirty;
        p.empty = empty;
        p.data = new byte[PageSize.getCurrent()];
        System.arraycopy(data, 0, p.data, 0, PageSize.getCurrent());
        return p;
    }

    public Page copy(int newSize) {
        Page p = new Page();
        p.pageNo = pageNo;
        p.dirty = dirty;
        p.empty = empty;
        p.data = new byte[newSize];
        System.arraycopy(data, 0, p.data, 0, data.length);
        return p;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(pageNo);
        out.writeBoolean(dirty);
        out.writeBoolean(empty);
        if (!empty)
            out.write(data, 0, PageSize.getCurrent());
    }

    public void read(DataInput in) throws IOException {
        pageNo = in.readInt();
        dirty = in.readBoolean();
        empty = in.readBoolean();
        data = new byte[PageSize.getCurrent()];
        if (!empty)
            in.readFully(data);
    }

    public String toString() {
        return "[Page, pageNo=" + pageNo + ", dirty=" + dirty + ", empty=" + empty + "]";
    }
}

