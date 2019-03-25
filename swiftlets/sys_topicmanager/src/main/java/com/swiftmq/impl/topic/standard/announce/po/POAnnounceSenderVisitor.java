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

package com.swiftmq.impl.topic.standard.announce.po;

import com.swiftmq.tools.pipeline.POVisitor;

public interface POAnnounceSenderVisitor extends POVisitor {
    public void visit(PODestinationAdded po);

    public void visit(PODestinationRemoved po);

    public void visit(PODestinationActivated po);

    public void visit(PODestinationDeactivated po);

    public void visit(POTopicCreated po);

    public void visit(POTopicRemoved po);

    public void visit(POSubscriptionAdded po);

    public void visit(POSubscriptionRemoved po);

    public void visit(POVersionNoteToSend po);

    public void visit(POVersionNoteReceived po);

    public void visit(POAnnounceSubscriptions po);
}
