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

package com.swiftmq.jms.smqp.v500;

import com.swiftmq.tools.requestreply.RequestVisitor;

public interface SMQPVisitor extends RequestVisitor {
    void visitAcknowledgeMessageRequest(AcknowledgeMessageRequest req);

    void visitAsyncMessageDeliveryRequest(AsyncMessageDeliveryRequest req);

    void visitCloseBrowserRequest(CloseBrowserRequest req);

    void visitCloseConsumerRequest(CloseConsumerRequest req);

    void visitCloseProducerRequest(CloseProducerRequest req);

    void visitCloseSessionRequest(CloseSessionRequest req);

    void visitCommitRequest(CommitRequest req);

    void visitCreateBrowserRequest(CreateBrowserRequest req);

    void visitCreateConsumerRequest(CreateConsumerRequest req);

    void visitCreateProducerRequest(CreateProducerRequest req);

    void visitCreatePublisherRequest(CreatePublisherRequest req);

    void visitCreateSessionRequest(CreateSessionRequest req);

    void visitCreateSubscriberRequest(CreateSubscriberRequest req);

    void visitCreateDurableRequest(CreateDurableRequest req);

    void visitDeleteDurableRequest(DeleteDurableRequest req);

    void visitCreateTmpQueueRequest(CreateTmpQueueRequest req);

    void visitDeleteTmpQueueRequest(DeleteTmpQueueRequest req);

    void visitFetchBrowserMessageRequest(FetchBrowserMessageRequest req);

    void visitGetClientIdRequest(GetClientIdRequest req);

    void visitSetClientIdRequest(SetClientIdRequest req);

    void visitGetMetaDataRequest(GetMetaDataRequest req);

    void visitProduceMessageRequest(ProduceMessageRequest req);

    void visitRecoverSessionRequest(RecoverSessionRequest req);

    void visitRollbackRequest(RollbackRequest req);

    void visitStartConsumerRequest(StartConsumerRequest req);

    void visitGetAuthChallengeRequest(GetAuthChallengeRequest req);

    void visitAuthResponseRequest(AuthResponseRequest req);

    void visitRouterConnectRequest(RouterConnectRequest req);

    void visitDisconnectRequest(DisconnectRequest req);

    void visitXAResRecoverRequest(XAResRecoverRequest req);

    void visitXAResStartRequest(XAResStartRequest req);

    void visitXAResEndRequest(XAResEndRequest req);

    void visitXAResPrepareRequest(XAResPrepareRequest req);

    void visitXAResCommitRequest(XAResCommitRequest req);

    void visitXAResRollbackRequest(XAResRollbackRequest req);

    void visitXAResSetTxTimeoutRequest(XAResSetTxTimeoutRequest req);

    void visitXAResGetTxTimeoutRequest(XAResGetTxTimeoutRequest req);

    void visitCreateShadowConsumerRequest(CreateShadowConsumerRequest req);

    void visitAssociateMessageRequest(AssociateMessageRequest req);

    void visitMessageDeliveredRequest(MessageDeliveredRequest req);
}

