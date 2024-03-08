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

package com.swiftmq.impl.topic.standard.announce;

import com.swiftmq.tools.versioning.VersionedConverter;
import com.swiftmq.tools.versioning.VersionedDumpable;
import com.swiftmq.tools.versioning.VersionedException;

public class TopicInfoConverter implements VersionedConverter {
    public void convert(VersionedDumpable vd, int toVersion) throws VersionedException {
        if (toVersion != 400)
            throw new VersionedException("Can't convert " + vd + " to version: " + toVersion);
    }

    public void convert(VersionedDumpable vd, int[] acceptedVersions) throws VersionedException {
        boolean found = false;
        for (int acceptedVersion : acceptedVersions) {
            if (acceptedVersion == 400) {
                found = true;
                break;
            }
        }
        if (!found)
            throw new VersionedException("Can't convert " + vd + " to one of the accepted versions");
    }
}
