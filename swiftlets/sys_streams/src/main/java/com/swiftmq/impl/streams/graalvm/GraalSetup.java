/*
 * Copyright 2020 IIT Software GmbH
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

package com.swiftmq.impl.streams.graalvm;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;

public class GraalSetup {
    private static HostAccess getHostAccess() {
        HostAccess.Builder builder = HostAccess.newBuilder(HostAccess.ALL);
        return builder.build();
    }

    public static Context context(ClassLoader cl) throws Exception {
        return Context.newBuilder("js")
                .allowExperimentalOptions(true)
                .option("js.nashorn-compat", "true")
                .option("js.ecmascript-version", "latest")
                .allowAllAccess(true)
                .allowHostAccess(getHostAccess())
                .hostClassLoader(cl).build();
    }
}
