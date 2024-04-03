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

package com.swiftmq.impl.mqtt.po;

import com.swiftmq.tools.pipeline.POVisitor;

public interface MQTTVisitor extends POVisitor {

    void visit(POConnect po);

    void visit(POPublish po);

    void visit(POSendMessage po);

    void visit(POSessionAssociated po);

    void visit(POPubAck po);

    void visit(POPubRec po);

    void visit(POPubRel po);

    void visit(POPubComp po);

    void visit(POSubscribe po);

    void visit(POUnsubscribe po);

    void visit(POPingReq po);

    void visit(PODisconnect po);

    void visit(POProtocolError po);

    void visit(POCollect po);

    void visit(POClose po);
}
