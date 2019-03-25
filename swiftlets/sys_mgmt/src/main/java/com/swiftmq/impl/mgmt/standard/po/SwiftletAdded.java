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

package com.swiftmq.impl.mgmt.standard.po;

import com.swiftmq.mgmt.Configuration;
import com.swiftmq.tools.pipeline.POVisitor;

public class SwiftletAdded extends EventObject {
    String name = null;
    Configuration config = null;

    public SwiftletAdded(String name, Configuration config) {
        this.name = name;
        this.config = config;
    }

    public String getName() {
        return name;
    }

    public Configuration getConfig() {
        return config;
    }

    public void accept(POVisitor visitor) {
        ((EventObjectVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[SwiftletAdded, name=" + name + ", config=<hidden>]";
    }
}
