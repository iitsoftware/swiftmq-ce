
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

public class InsertLogAction extends LogAction {
    int pageNo;

    /**
     * @param pageNo
     * @SBGen Constructor assigns pageNo
     */
    public InsertLogAction(int pageNo) {
        // SBgen: Assign variable
        this.pageNo = pageNo;
    }

    /**
     * @return
     */
    public int getType() {
        return INSERT;
    }

    /**
     * @return
     * @SBGen Method get pageNo
     */
    public int getPageNo() {
        // SBgen: Get variable
        return (pageNo);
    }

    /**
     * @param out
     * @throws IOException
     */
    protected void writeContent(DataOutput out)
            throws IOException {
        out.writeInt(pageNo);
    }

    /**
     * @param in
     * @throws IOException
     */
    protected void readContent(DataInput in)
            throws IOException {
        pageNo = in.readInt();
    }

    public String toString() {
        return "[InsertLogAction, pageNo=" + pageNo + "]";
    }
}

