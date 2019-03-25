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

package com.swiftmq;

import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.util.UpgradeUtilities;

public class Router {
    public static void main(java.lang.String[] args) {
        if (args.length == 0) {
            System.err.println("usage: java com.swiftmq.Router <configfile> [NOAMQP|<swiftmq-dir>]");
            System.exit(-1);
        }

        try {
            if (args.length > 1) {
                if (args[1].toLowerCase().equals("noamqp"))
                    UpgradeUtilities.upgrade_900_add_amqp_listeners = false;
                else
                    SwiftletManager.getInstance().setWorkingDirectory(args[1]);
            }
            SwiftletManager.getInstance().startRouter(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

