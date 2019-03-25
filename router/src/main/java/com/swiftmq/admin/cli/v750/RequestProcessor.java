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

package com.swiftmq.admin.cli.v750;

import com.swiftmq.admin.cli.CLI;
import com.swiftmq.mgmt.protocol.v750.*;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestService;

public class RequestProcessor extends ProtocolVisitorAdapter
        implements RequestService {
    CLI cli = null;

    public RequestProcessor(CLI cli) {
        this.cli = cli;
    }

    public void serviceRequest(Request request) {
        request.accept(this);
    }

    public void visit(BulkRequest request) {
        for (int i = 0; i < request.len; i++) {
            ((Request) request.dumpables[i]).accept(this);
        }
    }

    public void visit(RouterAvailableRequest request) {
        try {
            cli.markRouter(request.getRoutername(), true);
        } catch (Exception e) {
        }
    }

    public void visit(RouterUnavailableRequest request) {
        cli.markRouter(request.getRoutername(), false);
    }

    public void visit(DisconnectedRequest request) {
        if (!cli.isProgrammatic())
            System.out.println("Router '" + request.getRouterName() + "' has been disconnected! Reason: " + request.getReason());
        cli.markRouter(request.getRouterName(), false);
    }
}