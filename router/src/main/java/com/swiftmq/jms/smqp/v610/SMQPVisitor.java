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

package com.swiftmq.jms.smqp.v610;

/**
 * SMQP-Protocol Version 610, Class: SMQPVisitor
 * Automatically generated, don't change!
 * Generation Date: Mon Jul 17 17:50:11 CEST 2006
 * (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.RequestVisitor;

public interface SMQPVisitor extends RequestVisitor {
    void visit(AcknowledgeMessageRequest req);

    void visit(AssociateMessageRequest req);

    void visit(AsyncMessageDeliveryRequest req);

    void visit(AuthResponseRequest req);

    void visit(CloseBrowserRequest req);

    void visit(CloseConsumerRequest req);

    void visit(CloseProducerRequest req);

    void visit(CloseSessionRequest req);

    void visit(CommitRequest req);

    void visit(CreateBrowserRequest req);

    void visit(CreateConsumerRequest req);

    void visit(CreateDurableRequest req);

    void visit(CreateProducerRequest req);

    void visit(CreatePublisherRequest req);

    void visit(CreateSessionRequest req);

    void visit(CreateShadowConsumerRequest req);

    void visit(CreateSubscriberRequest req);

    void visit(CreateTmpQueueRequest req);

    void visit(DeleteDurableRequest req);

    void visit(DeleteMessageRequest req);

    void visit(DeleteTmpQueueRequest req);

    void visit(DisconnectRequest req);

    void visit(FetchBrowserMessageRequest req);

    void visit(GetAuthChallengeRequest req);

    void visit(GetClientIdRequest req);

    void visit(GetMetaDataRequest req);

    void visit(KeepAliveRequest req);

    void visit(MessageDeliveredRequest req);

    void visit(ProduceMessageRequest req);

    void visit(RecoverSessionRequest req);

    void visit(RollbackRequest req);

    void visit(RouterConnectRequest req);

    void visit(SetClientIdRequest req);

    void visit(StartConsumerRequest req);

    void visit(XAResCommitRequest req);

    void visit(XAResEndRequest req);

    void visit(XAResGetTxTimeoutRequest req);

    void visit(XAResPrepareRequest req);

    void visit(XAResRecoverRequest req);

    void visit(XAResRollbackRequest req);

    void visit(XAResSetTxTimeoutRequest req);

    void visit(XAResStartRequest req);
}