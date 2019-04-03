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

package com.swiftmq.upgrade;

import org.dom4j.Document;

public class UpgradeUtilities {
    private static Upgrader upgrader = null;

    public static void checkRelease(String configFilename, Document routerConfig) throws Exception {
        if (upgrader == null) {
            try {
                upgrader = (Upgrader)Class.forName("com.swiftmq.upgrade.UpgraderImpl").newInstance();
            } catch (Exception e) {
            }
        }
        if (upgrader != null)
            upgrader.upgradeConfig(configFilename, routerConfig);
    }
}