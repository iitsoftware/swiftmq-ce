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

import com.swiftmq.tools.requestreply.GenericRequest;
import com.swiftmq.tools.requestreply.Reply;

public class SMQPVisitorAdapter implements SMQPVisitor {
    public void visitAcknowledgeMessageRequest(AcknowledgeMessageRequest req) {
    }

    public void visitAsyncMessageDeliveryRequest(AsyncMessageDeliveryRequest req) {
    }

    public void visitCloseBrowserRequest(CloseBrowserRequest req) {
    }

    public void visitCloseConsumerRequest(CloseConsumerRequest req) {
    }

    public void visitCloseProducerRequest(CloseProducerRequest req) {
    }

    public void visitCloseSessionRequest(CloseSessionRequest req) {
    }

    public void visitCommitRequest(CommitRequest req) {
    }

    public void visitCreateBrowserRequest(CreateBrowserRequest req) {
    }

    public void visitCreateConsumerRequest(CreateConsumerRequest req) {
    }

    public void visitCreateProducerRequest(CreateProducerRequest req) {
    }

    public void visitCreatePublisherRequest(CreatePublisherRequest req) {
    }

    public void visitCreateSessionRequest(CreateSessionRequest req) {
    }

    public void visitCreateSubscriberRequest(CreateSubscriberRequest req) {
    }

    public void visitCreateDurableRequest(CreateDurableRequest req) {
    }

    public void visitDeleteDurableRequest(DeleteDurableRequest req) {
    }

    public void visitCreateTmpQueueRequest(CreateTmpQueueRequest req) {
    }

    public void visitDeleteTmpQueueRequest(DeleteTmpQueueRequest req) {
    }

    public void visitFetchBrowserMessageRequest(FetchBrowserMessageRequest req) {
    }

    public void visitGetClientIdRequest(GetClientIdRequest req) {
    }

    public void visitSetClientIdRequest(SetClientIdRequest req) {
    }

    public void visitGetMetaDataRequest(GetMetaDataRequest req) {
    }

    public void visitProduceMessageRequest(ProduceMessageRequest req) {
    }

    public void visitRecoverSessionRequest(RecoverSessionRequest req) {
    }

    public void visitRollbackRequest(RollbackRequest req) {
    }

    public void visitStartConsumerRequest(StartConsumerRequest req) {
    }

    public void visitGenericRequest(GenericRequest req) {
    }

    public void visitGetAuthChallengeRequest(GetAuthChallengeRequest req) {
    }

    public void visitAuthResponseRequest(AuthResponseRequest req) {
    }

    public void visitRouterConnectRequest(RouterConnectRequest req) {
    }

    public void visitDisconnectRequest(DisconnectRequest req) {
    }

    public void visitMessageDeliveredRequest(MessageDeliveredRequest req) {
    }

    public void visitXAResRecoverRequest(XAResRecoverRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visitXAResStartRequest(XAResStartRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visitXAResEndRequest(XAResEndRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visitXAResPrepareRequest(XAResPrepareRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visitXAResCommitRequest(XAResCommitRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visitXAResRollbackRequest(XAResRollbackRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visitXAResSetTxTimeoutRequest(XAResSetTxTimeoutRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visitXAResGetTxTimeoutRequest(XAResGetTxTimeoutRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visitCreateShadowConsumerRequest(CreateShadowConsumerRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visitAssociateMessageRequest(AssociateMessageRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }
}

