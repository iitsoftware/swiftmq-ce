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

package com.swiftmq.impl.topic.standard.announce.po;

import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;
import com.swiftmq.tools.versioning.VersionNotification;

public class POVersionNoteReceived extends POObject {
    VersionNotification versionNotification = null;

    public POVersionNoteReceived(VersionNotification versionNotification) {
        super(null, null);
        this.versionNotification = versionNotification;
    }

    public VersionNotification getVersionNotification() {
        return versionNotification;
    }

    public void accept(POVisitor visitor) {
        ((POAnnounceSenderVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[POVersionNoteReceived, versionNotification=" + versionNotification + "]";
    }
}
