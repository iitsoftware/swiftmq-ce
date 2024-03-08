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

package com.swiftmq.impl.store.standard.swap;

import com.swiftmq.swiftlet.store.StoreEntry;

public interface SwapFile {
    String getFilename();

    StoreEntry get(long fp) throws Exception;

    long add(StoreEntry entry) throws Exception;

    void remove(long fp) throws Exception;

    void updateDeliveryCount(long fp, int deliveryCount) throws Exception;

    boolean hasSpace() throws Exception;

    long getNumberMessages();

    void setNumberMessages(long numberMessages);

    long getMaxLength();

    void closeNoDelete();

    void close();
}

