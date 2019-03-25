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

package com.swiftmq.jndi.protocol.v400;

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;

public class JNDIRequestFactory extends DumpableFactory {
    public static final int LOOKUP = 0;
    public static final int BIND = 1;
    public static final int REBIND = 2;
    public static final int UNBIND = 3;

    public Dumpable createDumpable(int dumpId) {
        Dumpable d = null;
        switch (dumpId) {
            case LOOKUP:
                d = new LookupRequest();
                break;
            case BIND:
                d = new BindRequest();
                break;
            case REBIND:
                d = new RebindRequest();
                break;
            case UNBIND:
                d = new UnbindRequest();
                break;
        }
        return d;
    }
}
