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

/*--- formatted by Jindent 2.1, (www.c-lab.de/~jindent) ---*/

package com.swiftmq.jms.smqp.v400;

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;

public class SMQPFactory extends DumpableFactory {
    // DID start at 100
    public static final int DID_ACKNOWLEDGE_MESSAGE_REQ = 100;
    public static final int DID_ACKNOWLEDGE_MESSAGE_REP = 101;
    public static final int DID_ASYNC_MESSAGE_DELIVERY_REQ = 102;
    public static final int DID_ASYNC_MESSAGE_DELIVERY_REP = 103;
    public static final int DID_CLOSE_BROWSER_REQ = 104;
    public static final int DID_CLOSE_BROWSER_REP = 105;
    public static final int DID_CLOSE_CONSUMER_REQ = 106;
    public static final int DID_CLOSE_CONSUMER_REP = 107;
    public static final int DID_CLOSE_SESSION_REQ = 108;
    public static final int DID_CLOSE_SESSION_REP = 109;
    public static final int DID_COMMIT_REQ = 110;
    public static final int DID_COMMIT_REP = 111;
    public static final int DID_CREATE_BROWSER_REQ = 117;
    public static final int DID_CREATE_BROWSER_REP = 118;
    public static final int DID_CREATE_CONSUMER_REQ = 119;
    public static final int DID_CREATE_CONSUMER_REP = 120;
    public static final int DID_CREATE_SUBSCRIBER_REQ = 121;
    public static final int DID_CREATE_SUBSCRIBER_REP = 122;
    public static final int DID_CREATE_DURABLE_REQ = 123;
    public static final int DID_CREATE_DURABLE_REP = 124;
    public static final int DID_DELETE_DURABLE_REQ = 125;
    public static final int DID_DELETE_DURABLE_REP = 126;
    public static final int DID_CREATE_PRODUCER_REQ = 127;
    public static final int DID_CREATE_PRODUCER_REP = 128;
    public static final int DID_CREATE_PUBLISHER_REQ = 129;
    public static final int DID_CREATE_PUBLISHER_REP = 130;
    public static final int DID_CREATE_SESSION_REQ = 133;
    public static final int DID_CREATE_SESSION_REP = 134;
    public static final int DID_CREATE_TMP_QUEUE_REQ = 135;
    public static final int DID_CREATE_TMP_QUEUE_REP = 136;
    public static final int DID_DELETE_TMP_QUEUE_REQ = 137;
    public static final int DID_DELETE_TMP_QUEUE_REP = 138;
    public static final int DID_FETCH_BROWSER_MESSAGE_REQ = 139;
    public static final int DID_FETCH_BROWSER_MESSAGE_REP = 140;
    public static final int DID_GET_CLIENT_ID_REQ = 141;
    public static final int DID_GET_CLIENT_ID_REP = 142;
    public static final int DID_GET_META_DATA_REQ = 143;
    public static final int DID_GET_META_DATA_REP = 144;
    public static final int DID_PRODUCE_MESSAGE_REQ = 145;
    public static final int DID_PRODUCE_MESSAGE_REP = 146;
    public static final int DID_RECOVER_SESSION_REQ = 147;
    public static final int DID_RECOVER_SESSION_REP = 148;
    public static final int DID_ROLLBACK_REQ = 151;
    public static final int DID_ROLLBACK_REP = 152;
    public static final int DID_CLOSE_PRODUCER_REQ = 155;
    public static final int DID_CLOSE_PRODUCER_REP = 156;
    public static final int DID_SET_CLIENT_ID_REQ = 157;
    public static final int DID_SET_CLIENT_ID_REP = 158;
    public static final int DID_GET_AUTH_CHALLENGE_REQ = 159;
    public static final int DID_GET_AUTH_CHALLENGE_REP = 160;
    public static final int DID_AUTH_RESPONSE_REQ = 161;
    public static final int DID_AUTH_RESPONSE_REP = 162;
    public static final int DID_KEEP_ALIVE_REQ = 163;
    public static final int DID_ROUTER_CONNECT_REQ = 166;
    public static final int DID_ROUTER_CONNECT_REP = 167;
    public static final int DID_DISCONNECT_REQ = 169;
    public static final int DID_DISCONNECT_REP = 170;
    public static final int DID_XARESRECOVER_REQ = 171;
    public static final int DID_XARESRECOVER_REP = 172;
    public static final int DID_XARESSTART_REQ = 173;
    public static final int DID_XARESSTART_REP = 174;
    public static final int DID_XARESEND_REQ = 175;
    public static final int DID_XARESEND_REP = 176;
    public static final int DID_XARESPREPARE_REQ = 177;
    public static final int DID_XARESPREPARE_REP = 178;
    public static final int DID_XARESCOMMIT_REQ = 179;
    public static final int DID_XARESCOMMIT_REP = 180;
    public static final int DID_XARESROLLBACK_REQ = 181;
    public static final int DID_XARESROLLBACK_REP = 182;
    public static final int DID_CREATE_SHADOW_CONSUMER_REQ = 183;
    public static final int DID_CREATE_SHADOW_CONSUMER_REP = 184;
    public static final int DID_ASSOCIATE_MESSAGE_REQ = 185;
    public static final int DID_ASSOCIATE_MESSAGE_REP = 186;
    public static final int DID_BULK_REQ = 187;
    public static final int DID_START_CONSUMER_REQ = 188;
    public static final int DID_MESSAGE_DELIVERED_REQ = 189;

    /**
     * Creates a new Dumpable object for the given dump id.
     *
     * @param dumpId the dump id
     * @return Dumpable
     */
    public Dumpable createDumpable(int dumpId) {
        Dumpable dumpable = null;

        switch (dumpId) {

            case DID_ACKNOWLEDGE_MESSAGE_REQ:
                dumpable = new AcknowledgeMessageRequest(0, 0, null);
                break;

            case DID_ACKNOWLEDGE_MESSAGE_REP:
                dumpable = new AcknowledgeMessageReply();
                break;

            case DID_ASYNC_MESSAGE_DELIVERY_REQ:
                dumpable = new AsyncMessageDeliveryRequest(0, 0, null, 0);

                break;

            case DID_ASYNC_MESSAGE_DELIVERY_REP:
                dumpable = new AsyncMessageDeliveryReply(0);
                break;

            case DID_CLOSE_BROWSER_REQ:
                dumpable = new CloseBrowserRequest(0, 0);
                break;

            case DID_CLOSE_BROWSER_REP:
                dumpable = new CloseBrowserReply();
                break;

            case DID_CLOSE_CONSUMER_REQ:
                dumpable = new CloseConsumerRequest(0, 0, 0);
                break;

            case DID_CLOSE_CONSUMER_REP:
                dumpable = new CloseConsumerReply();
                break;

            case DID_CLOSE_SESSION_REQ:
                dumpable = new CloseSessionRequest(0);
                break;

            case DID_CLOSE_SESSION_REP:
                dumpable = new CloseSessionReply();
                break;

            case DID_COMMIT_REQ:
                dumpable = new CommitRequest(0);
                break;

            case DID_COMMIT_REP:
                dumpable = new CommitReply();
                break;

            case DID_CREATE_BROWSER_REQ:
                dumpable = new CreateBrowserRequest(0, null, null);
                break;

            case DID_CREATE_BROWSER_REP:
                dumpable = new CreateBrowserReply();
                break;

            case DID_CREATE_CONSUMER_REQ:
                dumpable = new CreateConsumerRequest(0, null, null);
                break;

            case DID_CREATE_CONSUMER_REP:
                dumpable = new CreateConsumerReply();
                break;

            case DID_CREATE_SUBSCRIBER_REQ:
                dumpable = new CreateSubscriberRequest(0, null, null, false);
                break;

            case DID_CREATE_SUBSCRIBER_REP:
                dumpable = new CreateSubscriberReply();
                break;

            case DID_CREATE_DURABLE_REQ:
                dumpable = new CreateDurableRequest(0, null, null, false, null);
                break;

            case DID_CREATE_DURABLE_REP:
                dumpable = new CreateDurableReply();
                break;

            case DID_DELETE_DURABLE_REQ:
                dumpable = new DeleteDurableRequest(0, null);
                break;

            case DID_DELETE_DURABLE_REP:
                dumpable = new DeleteDurableReply();
                break;

            case DID_CREATE_PRODUCER_REQ:
                dumpable = new CreateProducerRequest(0, null);
                break;

            case DID_CREATE_PRODUCER_REP:
                dumpable = new CreateProducerReply();
                break;

            case DID_CREATE_PUBLISHER_REQ:
                dumpable = new CreatePublisherRequest(0, null);
                break;

            case DID_CREATE_PUBLISHER_REP:
                dumpable = new CreatePublisherReply();
                break;

            case DID_CREATE_SESSION_REQ:
                dumpable = new CreateSessionRequest(false, 0, 0);
                break;

            case DID_CREATE_SESSION_REP:
                dumpable = new CreateSessionReply();
                break;

            case DID_CREATE_TMP_QUEUE_REQ:
                dumpable = new CreateTmpQueueRequest();
                break;

            case DID_CREATE_TMP_QUEUE_REP:
                dumpable = new CreateTmpQueueReply();

                break;

            case DID_DELETE_TMP_QUEUE_REQ:
                dumpable = new DeleteTmpQueueRequest(null);

                break;

            case DID_DELETE_TMP_QUEUE_REP:
                dumpable = new DeleteTmpQueueReply();

                break;

            case DID_FETCH_BROWSER_MESSAGE_REQ:
                dumpable = new FetchBrowserMessageRequest(0, 0, false);
                break;

            case DID_FETCH_BROWSER_MESSAGE_REP:
                dumpable = new FetchBrowserMessageReply();
                break;

            case DID_GET_CLIENT_ID_REQ:
                dumpable = new GetClientIdRequest();
                break;

            case DID_GET_CLIENT_ID_REP:
                dumpable = new GetClientIdReply();
                break;

            case DID_GET_META_DATA_REQ:
                dumpable = new GetMetaDataRequest();
                break;

            case DID_GET_META_DATA_REP:
                dumpable = new GetMetaDataReply();
                break;

            case DID_PRODUCE_MESSAGE_REQ:
                dumpable = new ProduceMessageRequest(0, 0, null, false);
                break;

            case DID_PRODUCE_MESSAGE_REP:
                dumpable = new ProduceMessageReply();
                break;

            case DID_RECOVER_SESSION_REQ:
                dumpable = new RecoverSessionRequest(0);
                break;

            case DID_RECOVER_SESSION_REP:
                dumpable = new RecoverSessionReply();
                break;

            case DID_ROLLBACK_REQ:
                dumpable = new RollbackRequest(0);
                break;

            case DID_ROLLBACK_REP:
                dumpable = new RollbackReply();
                break;

            case DID_CLOSE_PRODUCER_REQ:
                dumpable = new CloseProducerRequest(0, 0);
                break;

            case DID_CLOSE_PRODUCER_REP:
                dumpable = new CloseProducerReply();
                break;

            case DID_SET_CLIENT_ID_REQ:
                dumpable = new SetClientIdRequest();
                break;

            case DID_SET_CLIENT_ID_REP:
                dumpable = new SetClientIdReply();
                break;

            case DID_GET_AUTH_CHALLENGE_REQ:
                dumpable = new GetAuthChallengeRequest(null);
                break;

            case DID_GET_AUTH_CHALLENGE_REP:
                dumpable = new GetAuthChallengeReply();
                break;

            case DID_AUTH_RESPONSE_REQ:
                dumpable = new AuthResponseRequest(null);
                break;

            case DID_AUTH_RESPONSE_REP:
                dumpable = new AuthResponseReply();
                break;

            case DID_KEEP_ALIVE_REQ:
                dumpable = new KeepAliveRequest();
                break;

            case DID_ROUTER_CONNECT_REQ:
                dumpable = new RouterConnectRequest(null);
                break;

            case DID_ROUTER_CONNECT_REP:
                dumpable = new RouterConnectReply();
                break;

            case DID_DISCONNECT_REQ:
                dumpable = new DisconnectRequest();
                break;

            case DID_DISCONNECT_REP:
                dumpable = new DisconnectReply();
                break;

            case DID_XARESRECOVER_REQ:
                dumpable = new XAResRecoverRequest(0, 0);
                break;

            case DID_XARESRECOVER_REP:
                dumpable = new XAResRecoverReply();
                break;

            case DID_XARESSTART_REQ:
                dumpable = new XAResStartRequest(0, null, 0);
                break;

            case DID_XARESSTART_REP:
                dumpable = new XAResStartReply();
                break;

            case DID_XARESEND_REQ:
                dumpable = new XAResEndRequest(0, null, 0);
                break;

            case DID_XARESEND_REP:
                dumpable = new XAResEndReply();
                break;

            case DID_XARESPREPARE_REQ:
                dumpable = new XAResPrepareRequest(0, null, null);
                break;

            case DID_XARESPREPARE_REP:
                dumpable = new XAResPrepareReply();
                break;

            case DID_XARESCOMMIT_REQ:
                dumpable = new XAResCommitRequest(0, null, false);
                break;

            case DID_XARESCOMMIT_REP:
                dumpable = new XAResCommitReply();
                break;

            case DID_XARESROLLBACK_REQ:
                dumpable = new XAResRollbackRequest(0, null);
                break;

            case DID_XARESROLLBACK_REP:
                dumpable = new XAResRollbackReply();
                break;

            case DID_CREATE_SHADOW_CONSUMER_REQ:
                dumpable = new CreateShadowConsumerRequest(0, null);
                break;

            case DID_CREATE_SHADOW_CONSUMER_REP:
                dumpable = new CreateShadowConsumerReply();
                break;

            case DID_ASSOCIATE_MESSAGE_REQ:
                dumpable = new AssociateMessageRequest(0, null);
                break;

            case DID_ASSOCIATE_MESSAGE_REP:
                dumpable = new AssociateMessageReply();
                break;

            case DID_BULK_REQ:
                dumpable = new SMQPBulkRequest();
                break;

            case DID_START_CONSUMER_REQ:
                dumpable = new StartConsumerRequest(0, 0, 0, 0, 0);
                break;

            case DID_MESSAGE_DELIVERED_REQ:
                dumpable = new MessageDeliveredRequest(0, 0, null);
                break;
        }

        return (dumpable);
    }

}



