
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

import com.swiftmq.impl.store.standard.cache.Page;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UpdateLogAction extends LogAction {
    Page beforeImage;
    Page afterImage;

    /**
     * @param beforeImage
     * @param afterImage
     * @SBGen Constructor assigns beforeImage, afterImage
     */
    public UpdateLogAction(Page beforeImage, Page afterImage) {
        // SBgen: Assign variables
        this.beforeImage = beforeImage;
        this.afterImage = afterImage;
        // SBgen: End assign
    }

    /**
     * @return
     */
    public int getType() {
        return UPDATE;
    }

    /**
     * @return
     * @SBGen Method get beforeImage
     */
    public Page getBeforeImage() {
        // SBgen: Get variable
        return (beforeImage == null ? null : beforeImage.copy());
    }

    /**
     * @return
     * @SBGen Method get afterImage
     */
    public Page getAfterImage() {
        // SBgen: Get variable
        return (afterImage == null ? null : afterImage.copy());
    }

    /**
     * @param out
     * @throws IOException
     */
    protected void writeContent(DataOutput out)
            throws IOException {
        beforeImage.write(out);
        afterImage.write(out);
    }

    /**
     * @param in
     * @throws IOException
     */
    protected void readContent(DataInput in)
            throws IOException {
        beforeImage = new Page();
        beforeImage.read(in);
        afterImage = new Page();
        afterImage.read(in);
    }

    public String toString() {
        return "[UpdateLogAction, beforeImage=" + beforeImage + ", afterImage=" + afterImage + "]";
    }
}

