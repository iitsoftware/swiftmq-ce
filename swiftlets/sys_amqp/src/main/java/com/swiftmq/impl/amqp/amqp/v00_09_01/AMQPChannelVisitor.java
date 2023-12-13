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

package com.swiftmq.impl.amqp.amqp.v00_09_01;

import com.swiftmq.impl.amqp.amqp.v00_09_01.po.POActivateFlow;
import com.swiftmq.impl.amqp.amqp.v00_09_01.po.POChannelFrameReceived;
import com.swiftmq.impl.amqp.amqp.v00_09_01.po.POCloseChannel;
import com.swiftmq.impl.amqp.amqp.v00_09_01.po.POSendMessages;
import com.swiftmq.tools.pipeline.POVisitor;

public interface AMQPChannelVisitor extends POVisitor {
    void visit(POChannelFrameReceived po);

    void visit(POSendMessages po);

    void visit(POActivateFlow po);

    void visit(POCloseChannel po);
}
