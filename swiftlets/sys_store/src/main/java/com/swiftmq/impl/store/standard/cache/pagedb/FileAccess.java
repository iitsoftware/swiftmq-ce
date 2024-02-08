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

public interface FileAccess {
    void init() throws Exception;

    int numberPages();

    int freePages();

    int usedPages();

    long fileSize();

    Page initPage(int pageNo);

    boolean ensurePage(int pageNo) throws Exception;

    Page createPage() throws Exception;

    Page loadPage(int pageNo) throws Exception;

    void freePage(Page page) throws Exception;

    void writePage(Page p) throws Exception;

    void shrink() throws Exception;

    void sync() throws Exception;

    void reset() throws Exception;

    void close() throws Exception;

    void deleteStore() throws Exception;
}
