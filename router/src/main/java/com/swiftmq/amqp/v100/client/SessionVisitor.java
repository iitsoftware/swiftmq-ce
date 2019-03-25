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

public interface SessionVisitor extends POVisitor {
    public void visit(POBegin po);

    public void visit(POAttachProducer po);

    public void visit(POAttachConsumer po);

    public void visit(POAttachDurableConsumer po);

    public void visit(POSessionFrameReceived po);

    public void visit(POSendDisposition po);

    public void visit(POSendResumedTransfer po);

    public void visit(POFillCache po);

    public void visit(POSendMessage po);

    public void visit(POSendEnd po);

    public void visit(POCloseLink po);

    public void visit(POSessionClose po);
}