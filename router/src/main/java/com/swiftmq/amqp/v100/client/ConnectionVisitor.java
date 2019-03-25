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

package com.swiftmq.amqp.v100.client;

import com.swiftmq.amqp.v100.client.po.*;
import com.swiftmq.tools.pipeline.POVisitor;

public interface ConnectionVisitor extends POVisitor {
    public void visit(POProtocolRequest po);

    public void visit(POProtocolResponse po);

    public void visit(POAuthenticate po);

    public void visit(POOpen po);

    public void visit(POConnectionFrameReceived po);

    public void visit(POSendHeartBeat po);

    public void visit(POCheckIdleTimeout po);

    public void visit(POSendClose po);

    public void visit(POConnectionClose po);
}
