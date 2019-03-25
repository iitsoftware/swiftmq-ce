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

package com.swiftmq.tools.versioning;

import com.swiftmq.tools.dump.Dumpable;

public abstract class VersionedFactory {
    int[] supported = null;

    private String print(int[] a) {
        StringBuffer b = new StringBuffer("[");
        for (int i = 0; i < a.length; i++) {
            if (i > 0)
                b.append(", ");
            b.append(a[i]);
        }
        b.append("]");
        return b.toString();
    }

    protected abstract Dumpable createDumpable(Versioned versioned) throws VersionedException;

    public static int[] getSupportedVersions() {
        return null;
    }

    public Dumpable getDumpable(Versioned versioned) throws VersionedException {
        if (supported == null)
            supported = getSupportedVersions();
        boolean found = false;
        int version = versioned.getVersion();
        for (int i = 0; i < supported.length; i++) {
            if (version == supported[i]) {
                found = true;
                break;
            }
        }
        if (!found)
            throw new VersionedException("Requested version '" + version + " is not supported! Supported versions are: " + print(supported));
        return createDumpable(versioned);
    }
}
