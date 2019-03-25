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

package com.swiftmq.jms.smqp.v510;

/**
 * SMQP-Protocol Version 510, Class: SMQPVisitorAdapter
 * Automatically generated, don't change!
 * Generation Date: Fri Aug 13 16:00:44 CEST 2004
 * (c) 2004, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.GenericRequest;
import com.swiftmq.tools.requestreply.Reply;

public class SMQPVisitorAdapter implements SMQPVisitor {
    public void visit(AcknowledgeMessageRequest req) {
    }

    public void visit(AssociateMessageRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visit(AsyncMessageDeliveryRequest req) {
    }

    public void visit(AuthResponseRequest req) {
    }

    public void visit(CloseBrowserRequest req) {
    }

    public void visit(CloseConsumerRequest req) {
    }

    public void visit(CloseProducerRequest req) {
    }

    public void visit(CloseSessionRequest req) {
    }

    public void visit(CommitRequest req) {
    }

    public void visit(CreateBrowserRequest req) {
    }

    public void visit(CreateConsumerRequest req) {
    }

    public void visit(CreateDurableRequest req) {
    }

    public void visit(CreateProducerRequest req) {
    }

    public void visit(CreatePublisherRequest req) {
    }

    public void visit(CreateSessionRequest req) {
    }

    public void visit(CreateShadowConsumerRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visit(CreateSubscriberRequest req) {
    }

    public void visit(CreateTmpQueueRequest req) {
    }

    public void visit(DeleteDurableRequest req) {
    }

    public void visit(DeleteTmpQueueRequest req) {
    }

    public void visit(DisconnectRequest req) {
    }

    public void visit(FetchBrowserMessageRequest req) {
    }

    public void visit(GetAuthChallengeRequest req) {
    }

    public void visit(GetClientIdRequest req) {
    }

    public void visit(GetMetaDataRequest req) {
    }

    public void visit(KeepAliveRequest req) {
    }

    public void visit(MessageDeliveredRequest req) {
    }

    public void visit(ProduceMessageRequest req) {
    }

    public void visit(RecoverSessionRequest req) {
    }

    public void visit(RollbackRequest req) {
    }

    public void visit(RouterConnectRequest req) {
    }

    public void visit(SetClientIdRequest req) {
    }

    public void visit(StartConsumerRequest req) {
    }

    public void visit(XAResCommitRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visit(XAResEndRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visit(XAResGetTxTimeoutRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visit(XAResPrepareRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visit(XAResRecoverRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visit(XAResRollbackRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visit(XAResSetTxTimeoutRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }

    public void visit(XAResStartRequest req) {
        Reply reply = req.createReply();
        reply.setOk(false);
        reply.setException(new Exception("Please install the JMS XA/ASF Swiftlet to use this functionality!"));
        reply.send();
    }


    public void visitGenericRequest(GenericRequest req) {
    }
}