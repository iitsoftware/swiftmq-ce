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

import com.swiftmq.impl.store.standard.StoreContext;

public class SwapFileFactoryImpl implements SwapFileFactory {
    StoreContext ctx = null;

    public SwapFileFactoryImpl(StoreContext ctx) {
        this.ctx = ctx;
    }

    public SwapFile createSwapFile(String path, String filename, long maxLength) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/createSwapFile, filename=" + filename + ", maxLength=" + maxLength);
        return new SwapFileImpl(path, filename, maxLength);
    }

    public String toString() {
        return "SwapFileFactoryImpl";
    }
}
