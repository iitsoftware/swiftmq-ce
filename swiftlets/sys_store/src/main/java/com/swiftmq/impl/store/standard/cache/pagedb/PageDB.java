/*
 * Copyright 2024 IIT Software GmbH
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

package com.swiftmq.impl.store.standard.cache.pagedb;

import com.swiftmq.impl.store.standard.cache.Page;
import com.swiftmq.impl.store.standard.pagedb.PageSize;

import java.io.File;
import java.io.RandomAccessFile;

public class PageDB {
    public static final String FILENAME = "page.db";

    private static Page loadPage(RandomAccessFile file, int pageNo, int size) throws Exception {
        Page p = new Page();
        p.pageNo = pageNo;
        p.empty = true;
        p.dirty = false;
        p.data = new byte[size];
        long seekPoint = (long) pageNo * (long) size;
        file.seek(seekPoint);
        file.readFully(p.data);
        p.empty = p.data[0] == 1;
        return p;
    }

    private static void writePage(RandomAccessFile f, Page page, int newSize, byte[] empty) throws Exception {
        Page p = page.copy(newSize);
        if (p.empty)
            System.arraycopy(empty, 0, p.data, 0, empty.length);
        p.data[0] = (byte) (p.empty ? 1 : 0);
        f.seek((long) p.pageNo * (long) newSize);
        f.write(p.data);
        p.dirty = false;
    }

    public static void copyToNewSize(String sourceFilename, String destFilename, int oldSize, int newSize) throws Exception {
        RandomAccessFile sourceFile = new RandomAccessFile(sourceFilename, "r");
        int nPages = (int) (sourceFile.length() / oldSize);
        byte[] empty = makeEmptyArray(newSize);
        RandomAccessFile destFile = new RandomAccessFile(destFilename, "rw");
        for (int i = 0; i < nPages; i++) {
            writePage(destFile, loadPage(sourceFile, i, oldSize), newSize, empty);
        }
        destFile.getFD().sync();
        destFile.close();
        sourceFile.close();
    }

    public static void copy(String sourcePath, String destPath) throws Exception {
        String sourceFilename = sourcePath + File.separatorChar + FILENAME;
        RandomAccessFile sourceFile = new RandomAccessFile(sourceFilename, "r");
        int nPages = (int) (sourceFile.length() / PageSize.getCurrent());
        int shrinked = nPages;
        for (int i = nPages - 1; i >= 0; i--) {
            Page p = loadPage(sourceFile, i, PageSize.getCurrent());
            if (p.empty)
                shrinked--;
            else
                break;
        }
        byte[] empty = makeEmptyArray(PageSize.getCurrent());
        String destFilename = destPath + File.separatorChar + FILENAME;
        RandomAccessFile destFile = new RandomAccessFile(destFilename, "rw");
        for (int i = 0; i < shrinked; i++) {
            writePage(destFile, loadPage(sourceFile, i, PageSize.getCurrent()), PageSize.getCurrent(), empty);
        }
        destFile.getFD().sync();
        destFile.close();
        sourceFile.close();
    }

    public static byte[] makeEmptyArray(int size) {
        byte[] data = new byte[size];
        data[0] = 1; // emtpy
        for (int i = 1; i < size; i++)
            data[i] = 0;
        return data;
    }
}
