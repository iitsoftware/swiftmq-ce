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

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.cache.Page;
import com.swiftmq.impl.store.standard.log.UpdatePortionLogAction;

import java.util.Iterator;
import java.util.List;

public abstract class IndexPage implements Iterator {
    static final int POS_PREV_PAGE = Page.HEADER_LENGTH;
    static final int POS_NEXT_PAGE = POS_PREV_PAGE + 4;
    static final int POS_NENTRIES = POS_NEXT_PAGE + 4;
    static final int POS_NVALID = POS_NENTRIES + 4;
    static final int START_DATA = POS_NVALID + 4;

    StoreContext ctx = null;
    int pageNo = -1;
    Page page = null;
    Comparable minKey = null;
    Comparable maxKey = null;
    int nextPage = -1;
    int prevPage = -1;
    int nEntries = 0;
    int nValid = 0;
    int firstFree = START_DATA;
    int iterPos = 0;
    int iterElement = 0;
    int iterLastLength = 0;
    IndexEntry iterLastEntry = null;
    List journal = null;

    protected IndexPage(StoreContext ctx, int pageNo) {
        this.ctx = ctx;
        this.pageNo = pageNo;
        init();
    }

    private void init() {
        try {
            if (pageNo == -1) {
                page = ctx.cacheManager.createAndPin();
                pageNo = page.pageNo;
            } else {
                page = ctx.cacheManager.fetchAndPin(pageNo);
                initValues();
            }
        } catch (Exception e) {
            ctx.logSwiftlet.logError("sys$store", "pageNo " + pageNo + " seems to be damaged!");
            e.printStackTrace();
            throw new RuntimeException(e.toString());
        }
    }

    void initValues() {
        prevPage = Util.readInt(page.data, POS_PREV_PAGE);
        nextPage = Util.readInt(page.data, POS_NEXT_PAGE);
        nEntries = Util.readInt(page.data, POS_NENTRIES);
        nValid = Util.readInt(page.data, POS_NVALID);
        firstFree = START_DATA;
        IndexEntry entry = createIndexEntry();
        for (int i = 0; i < nEntries; i++) {
            entry.readContent(page.data, firstFree);
            firstFree += entry.getLength();
            if (entry.isValid()) {
                if (minKey == null || minKey.compareTo(entry.getKey()) > 0)
                    minKey = entry.getKey();
                if (maxKey == null || maxKey.compareTo(entry.getKey()) < 0)
                    maxKey = entry.getKey();
            }
        }
    }

    String getContent() {
        StringBuffer b = new StringBuffer();
        IndexEntry entry = createIndexEntry();
        int pos = START_DATA;
        for (int i = 0; i < nEntries; i++) {
            if (i > 0)
                b.append(',');
            entry.readContent(page.data, pos);
            b.append(entry.isValid() ? '+' : '-');
            b.append(entry.getKey());
            pos += entry.getLength();
        }
        return b.toString();
    }

    public void setJournal(List journal) {
        this.journal = journal;
    }

    public List getLogRecord() {
        return (journal);
    }

    public void load() {
        if (page == null) {
            try {
                page = ctx.cacheManager.fetchAndPin(pageNo);
            } catch (Exception e) {
                throw new RuntimeException(e.toString());
            }
        }
    }

    public void unload() throws Exception {
        if (page != null) {
            ctx.cacheManager.unpin(pageNo);
            page = null;
        }
        journal = null;
    }

    public Page getPage() {
        if (page == null)
            load();
        return (page);
    }

    int getNextPage() {
        return nextPage;
    }

    void setNextPage(int l) {
        if (page == null)
            load();
        nextPage = l;
        byte[] bi = new byte[4];
        System.arraycopy(page.data, POS_NEXT_PAGE, bi, 0, 4);

        Util.writeInt(nextPage, page.data, POS_NEXT_PAGE);
        page.dirty = true;
        page.empty = false;

        byte[] ai = new byte[4];
        System.arraycopy(page.data, POS_NEXT_PAGE, ai, 0, 4);
        journal.add(new UpdatePortionLogAction(pageNo, POS_NEXT_PAGE, bi, ai));
    }

    int getPrevPage() {
        return prevPage;
    }

    void setPrevPage(int l) {
        if (page == null)
            load();
        prevPage = l;
        byte[] bi = new byte[4];
        System.arraycopy(page.data, POS_PREV_PAGE, bi, 0, 4);

        Util.writeInt(prevPage, page.data, POS_PREV_PAGE);
        page.dirty = true;
        page.empty = false;

        byte[] ai = new byte[4];
        System.arraycopy(page.data, POS_PREV_PAGE, ai, 0, 4);
        journal.add(new UpdatePortionLogAction(pageNo, POS_PREV_PAGE, bi, ai));
    }

    int getNumberEntries() {
        if (page == null)
            load();
        return nEntries;
    }

    int getNumberValidEntries() {
        if (page == null)
            load();
        return nValid;
    }

    int getFirstFreePosition() {
        if (page == null)
            load();
        return firstFree;
    }

    int available() {
        if (page == null)
            load();
        return Page.PAGE_SIZE - firstFree;
    }

    /**
     * @param indexEntry
     */
    public void addEntry(IndexEntry indexEntry) {
        if (page == null)
            load();
        if (minKey == null || maxKey == null)
            selectMinMaxKey();
        boolean isLower = maxKey == null || maxKey.compareTo(indexEntry.getKey()) > 0;
        if (minKey == null || minKey.compareTo(indexEntry.getKey()) > 0)
            minKey = indexEntry.getKey();
        if (maxKey == null || maxKey.compareTo(indexEntry.getKey()) < 0)
            maxKey = indexEntry.getKey();
        byte[] bi = null;
        byte[] ai = null;
        int offset = 0;
        if (nEntries > 0 && isLower) {
            IndexEntry insertEntry = null;
            int pos = START_DATA;
            for (int i = 0; i < nEntries; i++) {
                IndexEntry act = createIndexEntry();
                act.readContent(page.data, pos);
                if (act.isValid() && act.getKey().compareTo(indexEntry.getKey()) > 0) {
                    insertEntry = act;
                    break;
                }
                pos += act.getLength();
            }
            if (insertEntry != null) {
                int amount = firstFree - pos;
                offset = pos;
                byte b[] = new byte[amount + indexEntry.getLength()];
                System.arraycopy(page.data, pos, b, 0, b.length);
                bi = b;
                System.arraycopy(b, 0, page.data, pos + indexEntry.getLength(), amount);
                indexEntry.writeContent(page.data, pos);
                ai = new byte[amount + indexEntry.getLength()];
                System.arraycopy(page.data, pos, ai, 0, ai.length);
            } else {
                // Before Image
                offset = firstFree;
                bi = new byte[indexEntry.getLength()];
                System.arraycopy(page.data, firstFree, bi, 0, indexEntry.getLength());

                // Action
                indexEntry.writeContent(page.data, firstFree);

                // After Image
                ai = new byte[indexEntry.getLength()];
                System.arraycopy(page.data, firstFree, ai, 0, indexEntry.getLength());
            }
        } else {
            // Before Image
            offset = firstFree;
            bi = new byte[indexEntry.getLength()];
            System.arraycopy(page.data, firstFree, bi, 0, indexEntry.getLength());

            // Action
            indexEntry.writeContent(page.data, firstFree);

            // After Image
            ai = new byte[indexEntry.getLength()];
            System.arraycopy(page.data, firstFree, ai, 0, indexEntry.getLength());
        }
        firstFree += indexEntry.getLength();

        journal.add(new UpdatePortionLogAction(pageNo, offset, bi, ai));

        // Before Image
        bi = new byte[4];
        System.arraycopy(page.data, POS_NENTRIES, bi, 0, 4);

        // Action
        nEntries++;
        Util.writeInt(nEntries, page.data, POS_NENTRIES);

        // After Image
        ai = new byte[4];
        System.arraycopy(page.data, POS_NENTRIES, ai, 0, 4);
        journal.add(new UpdatePortionLogAction(pageNo, POS_NENTRIES, bi, ai));

        // Before Image
        bi = new byte[4];
        System.arraycopy(page.data, POS_NVALID, bi, 0, 4);

        // Action
        nValid++;
        Util.writeInt(nValid, page.data, POS_NVALID);

        // After Image
        ai = new byte[4];
        System.arraycopy(page.data, POS_NVALID, ai, 0, 4);
        journal.add(new UpdatePortionLogAction(pageNo, POS_NVALID, bi, ai));

        page.dirty = true;
        page.empty = false;
    }

    /**
     * @return
     */
    Comparable getMinKey() {
        if (page == null)
            load();
        if (minKey == null && nEntries > 0)
            selectMinMaxKey();
        return minKey;
    }

    /**
     * @return
     */
    Comparable getMaxKey() {
        if (page == null)
            load();
        if (maxKey == null && nEntries > 0)
            selectMinMaxKey();
        return maxKey;
    }

    // Iterator implementation

    public Iterator iterator() {
        if (page == null)
            load();
        iterPos = START_DATA;
        iterElement = 0;
        iterLastLength = 0;
        return this;
    }

    public Object next() {
        IndexEntry entry = createIndexEntry();
        iterLastEntry = entry;
        entry.readContent(page.data, iterPos);
        iterLastLength = entry.getLength();
        iterPos += iterLastLength;
        iterElement++;
        return entry;
    }

    public boolean hasNext() {
        return iterElement < nEntries;
    }

    public void remove() {
        byte[] bi = null;
        byte[] ai = null;
        int offset = iterPos - iterLastLength;

        // Before Image
        bi = new byte[iterLastLength];
        System.arraycopy(page.data, offset, bi, 0, bi.length);

        // mark invalid
        iterLastEntry.setValid(false);
        iterLastEntry.writeContent(page.data, offset);

        // After Image
        ai = new byte[iterLastLength];
        System.arraycopy(page.data, offset, ai, 0, ai.length);
        journal.add(new UpdatePortionLogAction(pageNo, offset, bi, ai));

        // Before Image
        bi = new byte[4];
        System.arraycopy(page.data, POS_NVALID, bi, 0, 4);

        // Action
        nValid--;
        Util.writeInt(nValid, page.data, POS_NVALID);
        page.dirty = true;
        page.empty = nValid == 0;

        if (iterLastEntry.getKey().equals(minKey)) {
            minKey = null;
            if (nValid > 0) {
                IndexEntry firstValid = getFirstValid(iterPos);
                if (firstValid != null) {
                    minKey = firstValid.getKey();
                }
            }
        }

        if (iterLastEntry.getKey().equals(maxKey))
            maxKey = null;

        // After Image
        ai = new byte[4];
        System.arraycopy(page.data, POS_NVALID, ai, 0, 4);
        journal.add(new UpdatePortionLogAction(pageNo, POS_NVALID, bi, ai));

        if (page.empty) {
            // Before Image
            bi = new byte[4];
            System.arraycopy(page.data, POS_NENTRIES, bi, 0, 4);

            // Action
            nEntries = 0;
            Util.writeInt(nEntries, page.data, POS_NENTRIES);

            // After Image
            ai = new byte[4];
            System.arraycopy(page.data, POS_NENTRIES, ai, 0, 4);
            journal.add(new UpdatePortionLogAction(pageNo, POS_NENTRIES, bi, ai));

            firstFree = START_DATA;
        }
    }

    public void replace(IndexEntry newEntry) {
        byte[] bi = null;
        byte[] ai = null;
        int offset = iterPos - iterLastLength;

        // Before Image
        bi = new byte[iterLastLength];
        System.arraycopy(page.data, offset, bi, 0, bi.length);

        // replace
        newEntry.writeContent(page.data, offset);
        iterLastEntry = newEntry;

        // After Image
        ai = new byte[iterLastLength];
        System.arraycopy(page.data, offset, ai, 0, ai.length);
        journal.add(new UpdatePortionLogAction(pageNo, offset, bi, ai));
    }

    protected abstract IndexEntry createIndexEntry();

    private IndexEntry getFirstValid(int pos) {
        IndexEntry entry = createIndexEntry();
        while (true) {
            entry.readContent(page.data, pos);
            pos += entry.getLength();
            if (entry.isValid())
                return entry;
        }
    }

    private void selectMinMaxKey() {
        int pos = START_DATA;
        IndexEntry entry = createIndexEntry();
        for (int i = 0; i < nEntries; i++) {
            entry.readContent(page.data, pos);
            pos += entry.getLength();
            if (entry.isValid()) {
                if (minKey == null || minKey.compareTo(entry.getKey()) > 0)
                    minKey = entry.getKey();
                if (maxKey == null || maxKey.compareTo(entry.getKey()) < 0)
                    maxKey = entry.getKey();
            }
        }
    }

    public String toString() {
        return ", pageNo=" + pageNo + ", page=" + page + ", minKey=" + minKey + ", maxKey=" + maxKey + ", prevPage=" + prevPage + ", nextPage=" + nextPage + ", nEntries=" + nEntries + ", nValid=" + nValid + ", firstFree=" + firstFree + ", available=" + available();
    }
}

