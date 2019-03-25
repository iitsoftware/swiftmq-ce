
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

public class DeleteLogAction extends LogAction {
    int pageNo;
    byte[] beforeImage;

    /**
     * @param beforeImage
     * @SBGen Constructor assigns beforeImage
     */
    public DeleteLogAction(int pageNo, byte[] beforeImage) {
        // SBgen: Assign variable
        this.pageNo = pageNo;
        this.beforeImage = beforeImage;
    }

    /**
     * @return
     */
    public int getType() {
        return DELETE;
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
     * @return
     * @SBGen Method get beforeImage
     */
    public byte[] getBeforeImage() {
        // SBgen: Get variable
        return (beforeImage == null ? null : (byte[]) beforeImage.clone());
    }

    /**
     * @param out
     * @throws IOException
     */
    protected void writeContent(DataOutput out)
            throws IOException {
        out.writeInt(pageNo);
        out.writeInt(beforeImage.length);
        out.write(beforeImage);
    }

    /**
     * @param in
     * @throws IOException
     */
    protected void readContent(DataInput in)
            throws IOException {
        pageNo = in.readInt();
        beforeImage = new byte[in.readInt()];
        in.readFully(beforeImage);
    }

    public String toString() {
        return "[DeleteLogAction, pageNo=" + pageNo + ", beforeImage=" + beforeImage + "]";
    }
}

