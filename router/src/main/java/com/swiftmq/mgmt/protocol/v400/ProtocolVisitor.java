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

package com.swiftmq.mgmt.protocol.v400;

import com.swiftmq.tools.requestreply.RequestVisitor;

public interface ProtocolVisitor extends RequestVisitor {
    public void visit(AuthRequest request);

    public void visit(BulkRequest request);

    public void visit(CommandRequest request);

    public void visit(ConnectRequest request);

    public void visit(EntityAddedRequest request);

    public void visit(EntityRemovedRequest request);

    public void visit(SwiftletAddedRequest request);

    public void visit(SwiftletRemovedRequest request);

    public void visit(LeaseRequest request);

    public void visit(PropertyChangedRequest request);

    public void visit(RouterAvailableRequest request);

    public void visit(RouterUnavailableRequest request);

    public void visit(RouterConfigRequest request);

    public void visit(DisconnectedRequest request);
}
