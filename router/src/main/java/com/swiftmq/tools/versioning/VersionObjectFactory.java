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
import com.swiftmq.tools.dump.DumpableFactory;

public class VersionObjectFactory extends DumpableFactory {
    public static final int VERSION_NOTIFICATION = 0;
    public static final int VERSIONED = 1;

    public Dumpable createDumpable(int dumpId) {
        Dumpable d = null;
        switch (dumpId) {
            case VERSION_NOTIFICATION:
                d = new VersionNotification();
                break;
            case VERSIONED:
                d = new Versioned();
                break;
        }
        return d;
    }
}
