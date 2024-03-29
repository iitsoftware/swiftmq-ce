
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

import com.swiftmq.impl.store.standard.cache.Page;

import java.io.DataOutput;

public class MessagePage {
    static final int POS_PREV_PAGE = Page.HEADER_LENGTH;
    static final int POS_NEXT_PAGE = POS_PREV_PAGE + 4;
    static final int POS_LENGTH = POS_NEXT_PAGE + 4;
    public static final int START_DATA = POS_LENGTH + 4;

    int nextPage = -1;
    int prevPage = -1;
    int length = 0;

    public Page page = null;

    public MessagePage(Page page) {
        this.page = page;
        prevPage = Util.readInt(page.data, POS_PREV_PAGE);
        nextPage = Util.readInt(page.data, POS_NEXT_PAGE);
        length = Util.readInt(page.data, POS_LENGTH);
    }

    public int getNextPage() {
        return nextPage;
    }

    public void setNextPage(int l) {
        nextPage = l;
        Util.writeInt(nextPage, page.data, POS_NEXT_PAGE);
    }

    public int getPrevPage() {
        return prevPage;
    }

    public void setPrevPage(int l) {
        prevPage = l;
        Util.writeInt(prevPage, page.data, POS_PREV_PAGE);
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
        Util.writeInt(length, page.data, POS_LENGTH);
    }

    public void writeData(DataOutput dbos) throws Exception {
        int len = getLength() - START_DATA;
        dbos.write(page.data, START_DATA, len);
    }

    public String toString() {
        return "[MessagePage, length=" + length + ", prevPage=" + prevPage + ", nextPage=" + nextPage + ", page=" + page + "]";
    }
}

