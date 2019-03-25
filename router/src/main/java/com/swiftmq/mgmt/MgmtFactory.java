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

package com.swiftmq.mgmt;

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;

public class MgmtFactory extends DumpableFactory {
    public static final int ENTITY = 0;
    public static final int ENTITYLIST = 1;
    public static final int PROPERTY = 2;
    public static final int CONFIGURATION = 3;
    public static final int CONFIGINSTANCE = 4;
    public static final int META = 5;
    public static final int COMMANDREGISTRY = 6;
    public static final int COMMAND = 7;

    public Dumpable createDumpable(int dumpId) {
        Dumpable d = null;

        switch (dumpId) {
            case ENTITY:
                d = new Entity();
                break;
            case ENTITYLIST:
                d = new EntityList();
                break;
            case PROPERTY:
                d = new Property();
                break;
            case CONFIGURATION:
                d = new Configuration();
                break;
            case CONFIGINSTANCE:
                d = new RouterConfigInstance();
                break;
            case META:
                d = new MetaData();
                break;
            case COMMANDREGISTRY:
                d = new CommandRegistry();
                break;
            case COMMAND:
                d = new Command();
                break;
        }
        return d;
    }
}
