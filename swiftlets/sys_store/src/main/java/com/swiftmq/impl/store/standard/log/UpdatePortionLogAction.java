
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

public class UpdatePortionLogAction extends LogAction {
    int pageNo;
    int offset;
    byte[] beforeImage;
    byte[] afterImage;

    /**
     * @param pageNo
     * @param offset
     * @param beforeImage
     * @param afterImage
     * @SBGen Constructor assigns pageNo, offset, beforeImage, afterImage
     */
    public UpdatePortionLogAction(int pageNo, int offset, byte[] beforeImage, byte[] afterImage) {
        // SBgen: Assign variables
        this.pageNo = pageNo;
        this.offset = offset;
        this.beforeImage = beforeImage;
        this.afterImage = afterImage;
        // SBgen: End assign
    }

    /**
     * @return
     */
    public int getType() {
        return UPDATE_PORTION;
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
     * @SBGen Method get offset
     */
    public int getOffset() {
        // SBgen: Get variable
        return (offset);
    }

    /**
     * @return
     * @SBGen Method get beforeImage
     */
    public byte[] getBeforeImage() {
        // SBgen: Get variable
        return (beforeImage);
    }

    /**
     * @return
     * @SBGen Method get afterImage
     */
    public byte[] getAfterImage() {
        // SBgen: Get variable
        return (afterImage);
    }

    /**
     * @param out
     * @throws IOException
     */
    protected void writeContent(DataOutput out)
            throws IOException {
        out.writeInt(pageNo);
        out.writeInt(offset);
        if (beforeImage != null) {
            out.writeByte(1);
            out.writeInt(beforeImage.length);
            out.write(beforeImage);
        } else
            out.writeByte(0);
        out.writeInt(afterImage.length);
        out.write(afterImage);
    }

    /**
     * @param in
     * @throws IOException
     */
    protected void readContent(DataInput in)
            throws IOException {
        pageNo = in.readInt();
        offset = in.readInt();
        int set = in.readByte();
        if (set == 1) {
            beforeImage = new byte[in.readInt()];
            in.readFully(beforeImage);
        } else
            beforeImage = null;
        afterImage = new byte[in.readInt()];
        in.readFully(afterImage);
    }

    public String toString() {
        return "[UpdateLogPortionAction, pageNo=" + pageNo + ", offset=" + offset + ", length=" + afterImage.length + "]";
    }
}

