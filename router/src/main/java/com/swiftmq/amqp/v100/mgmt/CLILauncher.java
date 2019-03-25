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

package com.swiftmq.amqp.v100.mgmt;

import com.swiftmq.admin.cli.CLI;

public class CLILauncher {
    public static void main(String[] args) {
        if (args.length == 0 || args.length > 2) {
            System.out.println();
            System.out.println("usage: java com.swiftmq.amqp.v100.mgmt.CLILauncher <amqp-url> [<scriptfile>]");
            System.out.println();
            System.out.println("<amqp-url>   is the AMQP-URL like 'amqp://localhost:5672'");
            System.out.println("<scriptfile> name of an optional file with CLI commands");
            System.out.println();
            System.out.println("See SwiftMQ's documentation for details");
            System.out.println();
            System.exit(-1);
        }

        try {
            new CLI(new AMQPConnectionHolder(args[0]), args.length == 2 ? args[1] : null).run();
        } catch (Exception e) {
            System.out.println(e);
            System.exit(-1);
        }

    }
}
