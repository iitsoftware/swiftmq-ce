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

package com.swiftmq.jms.smqp.v750;

/**
 * SMQP-Protocol Version 750, Class: SMQPFactory
 * Automatically generated, don't change!
 * Generation Date: Tue Apr 21 10:39:21 CEST 2009
 * (c) 2009, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;

public class SMQPFactory extends DumpableFactory {
    public static final int DID_BULK_REQ = 100;
    public static final int DID_ACKNOWLEDGEMESSAGE_REQ = 101;
    public static final int DID_ACKNOWLEDGEMESSAGE_REP = 102;
    public static final int DID_ASSOCIATEMESSAGE_REQ = 103;
    public static final int DID_ASSOCIATEMESSAGE_REP = 104;
    public static final int DID_ASYNCMESSAGEDELIVERY_REQ = 105;
    public static final int DID_ASYNCMESSAGEDELIVERY_REP = 106;
    public static final int DID_AUTHRESPONSE_REQ = 107;
    public static final int DID_AUTHRESPONSE_REP = 108;
    public static final int DID_CLOSEBROWSER_REQ = 109;
    public static final int DID_CLOSEBROWSER_REP = 110;
    public static final int DID_CLOSECONSUMER_REQ = 111;
    public static final int DID_CLOSECONSUMER_REP = 112;
    public static final int DID_CLOSEPRODUCER_REQ = 113;
    public static final int DID_CLOSEPRODUCER_REP = 114;
    public static final int DID_CLOSESESSION_REQ = 115;
    public static final int DID_CLOSESESSION_REP = 116;
    public static final int DID_COMMIT_REQ = 117;
    public static final int DID_COMMIT_REP = 118;
    public static final int DID_CREATEBROWSER_REQ = 119;
    public static final int DID_CREATEBROWSER_REP = 120;
    public static final int DID_CREATECONSUMER_REQ = 121;
    public static final int DID_CREATECONSUMER_REP = 122;
    public static final int DID_CREATEDURABLE_REQ = 123;
    public static final int DID_CREATEDURABLE_REP = 124;
    public static final int DID_CREATEPRODUCER_REQ = 125;
    public static final int DID_CREATEPRODUCER_REP = 126;
    public static final int DID_CREATEPUBLISHER_REQ = 127;
    public static final int DID_CREATEPUBLISHER_REP = 128;
    public static final int DID_CREATESESSION_REQ = 129;
    public static final int DID_CREATESESSION_REP = 130;
    public static final int DID_CREATESHADOWCONSUMER_REQ = 131;
    public static final int DID_CREATESHADOWCONSUMER_REP = 132;
    public static final int DID_CREATESUBSCRIBER_REQ = 133;
    public static final int DID_CREATESUBSCRIBER_REP = 134;
    public static final int DID_CREATETMPQUEUE_REQ = 135;
    public static final int DID_CREATETMPQUEUE_REP = 136;
    public static final int DID_DELETEDURABLE_REQ = 137;
    public static final int DID_DELETEDURABLE_REP = 138;
    public static final int DID_DELETEMESSAGE_REQ = 139;
    public static final int DID_DELETETMPQUEUE_REQ = 140;
    public static final int DID_DELETETMPQUEUE_REP = 141;
    public static final int DID_DISCONNECT_REQ = 142;
    public static final int DID_DISCONNECT_REP = 143;
    public static final int DID_FETCHBROWSERMESSAGE_REQ = 144;
    public static final int DID_FETCHBROWSERMESSAGE_REP = 145;
    public static final int DID_GETAUTHCHALLENGE_REQ = 146;
    public static final int DID_GETAUTHCHALLENGE_REP = 147;
    public static final int DID_GETCLIENTID_REQ = 148;
    public static final int DID_GETCLIENTID_REP = 149;
    public static final int DID_GETMETADATA_REQ = 150;
    public static final int DID_GETMETADATA_REP = 151;
    public static final int DID_KEEPALIVE_REQ = 152;
    public static final int DID_MESSAGEDELIVERED_REQ = 153;
    public static final int DID_PRODUCEMESSAGE_REQ = 154;
    public static final int DID_PRODUCEMESSAGE_REP = 155;
    public static final int DID_RECOVERSESSION_REQ = 156;
    public static final int DID_RECOVERSESSION_REP = 157;
    public static final int DID_ROLLBACK_REQ = 158;
    public static final int DID_ROLLBACK_REP = 159;
    public static final int DID_ROUTERCONNECT_REQ = 160;
    public static final int DID_ROUTERCONNECT_REP = 161;
    public static final int DID_SETCLIENTID_REQ = 162;
    public static final int DID_SETCLIENTID_REP = 163;
    public static final int DID_STARTCONSUMER_REQ = 164;
    public static final int DID_XARESCOMMIT_REQ = 165;
    public static final int DID_XARESCOMMIT_REP = 166;
    public static final int DID_XARESEND_REQ = 167;
    public static final int DID_XARESEND_REP = 168;
    public static final int DID_XARESFORGET_REQ = 169;
    public static final int DID_XARESFORGET_REP = 170;
    public static final int DID_XARESGETTXTIMEOUT_REQ = 171;
    public static final int DID_XARESGETTXTIMEOUT_REP = 172;
    public static final int DID_XARESPREPARE_REQ = 173;
    public static final int DID_XARESPREPARE_REP = 174;
    public static final int DID_XARESRECOVER_REQ = 175;
    public static final int DID_XARESRECOVER_REP = 176;
    public static final int DID_XARESROLLBACK_REQ = 177;
    public static final int DID_XARESROLLBACK_REP = 178;
    public static final int DID_XARESSETTXTIMEOUT_REQ = 179;
    public static final int DID_XARESSETTXTIMEOUT_REP = 180;
    public static final int DID_XARESSTART_REQ = 181;
    public static final int DID_XARESSTART_REP = 182;

    public Dumpable createDumpable(int dumpId) {
        Dumpable dumpable = null;

        switch (dumpId) {
            case DID_BULK_REQ:
                dumpable = new SMQPBulkRequest();
                break;
            case DID_ACKNOWLEDGEMESSAGE_REQ:
                dumpable = new AcknowledgeMessageRequest();
                break;
            case DID_ACKNOWLEDGEMESSAGE_REP:
                dumpable = new AcknowledgeMessageReply();
                break;
            case DID_ASSOCIATEMESSAGE_REQ:
                dumpable = new AssociateMessageRequest();
                break;
            case DID_ASSOCIATEMESSAGE_REP:
                dumpable = new AssociateMessageReply();
                break;
            case DID_ASYNCMESSAGEDELIVERY_REQ:
                dumpable = new AsyncMessageDeliveryRequest();
                break;
            case DID_ASYNCMESSAGEDELIVERY_REP:
                dumpable = new AsyncMessageDeliveryReply();
                break;
            case DID_AUTHRESPONSE_REQ:
                dumpable = new AuthResponseRequest();
                break;
            case DID_AUTHRESPONSE_REP:
                dumpable = new AuthResponseReply();
                break;
            case DID_CLOSEBROWSER_REQ:
                dumpable = new CloseBrowserRequest();
                break;
            case DID_CLOSEBROWSER_REP:
                dumpable = new CloseBrowserReply();
                break;
            case DID_CLOSECONSUMER_REQ:
                dumpable = new CloseConsumerRequest();
                break;
            case DID_CLOSECONSUMER_REP:
                dumpable = new CloseConsumerReply();
                break;
            case DID_CLOSEPRODUCER_REQ:
                dumpable = new CloseProducerRequest();
                break;
            case DID_CLOSEPRODUCER_REP:
                dumpable = new CloseProducerReply();
                break;
            case DID_CLOSESESSION_REQ:
                dumpable = new CloseSessionRequest();
                break;
            case DID_CLOSESESSION_REP:
                dumpable = new CloseSessionReply();
                break;
            case DID_COMMIT_REQ:
                dumpable = new CommitRequest();
                break;
            case DID_COMMIT_REP:
                dumpable = new CommitReply();
                break;
            case DID_CREATEBROWSER_REQ:
                dumpable = new CreateBrowserRequest();
                break;
            case DID_CREATEBROWSER_REP:
                dumpable = new CreateBrowserReply();
                break;
            case DID_CREATECONSUMER_REQ:
                dumpable = new CreateConsumerRequest();
                break;
            case DID_CREATECONSUMER_REP:
                dumpable = new CreateConsumerReply();
                break;
            case DID_CREATEDURABLE_REQ:
                dumpable = new CreateDurableRequest();
                break;
            case DID_CREATEDURABLE_REP:
                dumpable = new CreateDurableReply();
                break;
            case DID_CREATEPRODUCER_REQ:
                dumpable = new CreateProducerRequest();
                break;
            case DID_CREATEPRODUCER_REP:
                dumpable = new CreateProducerReply();
                break;
            case DID_CREATEPUBLISHER_REQ:
                dumpable = new CreatePublisherRequest();
                break;
            case DID_CREATEPUBLISHER_REP:
                dumpable = new CreatePublisherReply();
                break;
            case DID_CREATESESSION_REQ:
                dumpable = new CreateSessionRequest();
                break;
            case DID_CREATESESSION_REP:
                dumpable = new CreateSessionReply();
                break;
            case DID_CREATESHADOWCONSUMER_REQ:
                dumpable = new CreateShadowConsumerRequest();
                break;
            case DID_CREATESHADOWCONSUMER_REP:
                dumpable = new CreateShadowConsumerReply();
                break;
            case DID_CREATESUBSCRIBER_REQ:
                dumpable = new CreateSubscriberRequest();
                break;
            case DID_CREATESUBSCRIBER_REP:
                dumpable = new CreateSubscriberReply();
                break;
            case DID_CREATETMPQUEUE_REQ:
                dumpable = new CreateTmpQueueRequest();
                break;
            case DID_CREATETMPQUEUE_REP:
                dumpable = new CreateTmpQueueReply();
                break;
            case DID_DELETEDURABLE_REQ:
                dumpable = new DeleteDurableRequest();
                break;
            case DID_DELETEDURABLE_REP:
                dumpable = new DeleteDurableReply();
                break;
            case DID_DELETEMESSAGE_REQ:
                dumpable = new DeleteMessageRequest();
                break;
            case DID_DELETETMPQUEUE_REQ:
                dumpable = new DeleteTmpQueueRequest();
                break;
            case DID_DELETETMPQUEUE_REP:
                dumpable = new DeleteTmpQueueReply();
                break;
            case DID_DISCONNECT_REQ:
                dumpable = new DisconnectRequest();
                break;
            case DID_DISCONNECT_REP:
                dumpable = new DisconnectReply();
                break;
            case DID_FETCHBROWSERMESSAGE_REQ:
                dumpable = new FetchBrowserMessageRequest();
                break;
            case DID_FETCHBROWSERMESSAGE_REP:
                dumpable = new FetchBrowserMessageReply();
                break;
            case DID_GETAUTHCHALLENGE_REQ:
                dumpable = new GetAuthChallengeRequest();
                break;
            case DID_GETAUTHCHALLENGE_REP:
                dumpable = new GetAuthChallengeReply();
                break;
            case DID_GETCLIENTID_REQ:
                dumpable = new GetClientIdRequest();
                break;
            case DID_GETCLIENTID_REP:
                dumpable = new GetClientIdReply();
                break;
            case DID_GETMETADATA_REQ:
                dumpable = new GetMetaDataRequest();
                break;
            case DID_GETMETADATA_REP:
                dumpable = new GetMetaDataReply();
                break;
            case DID_KEEPALIVE_REQ:
                dumpable = new KeepAliveRequest();
                break;
            case DID_MESSAGEDELIVERED_REQ:
                dumpable = new MessageDeliveredRequest();
                break;
            case DID_PRODUCEMESSAGE_REQ:
                dumpable = new ProduceMessageRequest();
                break;
            case DID_PRODUCEMESSAGE_REP:
                dumpable = new ProduceMessageReply();
                break;
            case DID_RECOVERSESSION_REQ:
                dumpable = new RecoverSessionRequest();
                break;
            case DID_RECOVERSESSION_REP:
                dumpable = new RecoverSessionReply();
                break;
            case DID_ROLLBACK_REQ:
                dumpable = new RollbackRequest();
                break;
            case DID_ROLLBACK_REP:
                dumpable = new RollbackReply();
                break;
            case DID_ROUTERCONNECT_REQ:
                dumpable = new RouterConnectRequest();
                break;
            case DID_ROUTERCONNECT_REP:
                dumpable = new RouterConnectReply();
                break;
            case DID_SETCLIENTID_REQ:
                dumpable = new SetClientIdRequest();
                break;
            case DID_SETCLIENTID_REP:
                dumpable = new SetClientIdReply();
                break;
            case DID_STARTCONSUMER_REQ:
                dumpable = new StartConsumerRequest();
                break;
            case DID_XARESCOMMIT_REQ:
                dumpable = new XAResCommitRequest();
                break;
            case DID_XARESCOMMIT_REP:
                dumpable = new XAResCommitReply();
                break;
            case DID_XARESEND_REQ:
                dumpable = new XAResEndRequest();
                break;
            case DID_XARESEND_REP:
                dumpable = new XAResEndReply();
                break;
            case DID_XARESFORGET_REQ:
                dumpable = new XAResForgetRequest();
                break;
            case DID_XARESFORGET_REP:
                dumpable = new XAResForgetReply();
                break;
            case DID_XARESGETTXTIMEOUT_REQ:
                dumpable = new XAResGetTxTimeoutRequest();
                break;
            case DID_XARESGETTXTIMEOUT_REP:
                dumpable = new XAResGetTxTimeoutReply();
                break;
            case DID_XARESPREPARE_REQ:
                dumpable = new XAResPrepareRequest();
                break;
            case DID_XARESPREPARE_REP:
                dumpable = new XAResPrepareReply();
                break;
            case DID_XARESRECOVER_REQ:
                dumpable = new XAResRecoverRequest();
                break;
            case DID_XARESRECOVER_REP:
                dumpable = new XAResRecoverReply();
                break;
            case DID_XARESROLLBACK_REQ:
                dumpable = new XAResRollbackRequest();
                break;
            case DID_XARESROLLBACK_REP:
                dumpable = new XAResRollbackReply();
                break;
            case DID_XARESSETTXTIMEOUT_REQ:
                dumpable = new XAResSetTxTimeoutRequest();
                break;
            case DID_XARESSETTXTIMEOUT_REP:
                dumpable = new XAResSetTxTimeoutReply();
                break;
            case DID_XARESSTART_REQ:
                dumpable = new XAResStartRequest();
                break;
            case DID_XARESSTART_REP:
                dumpable = new XAResStartReply();
                break;
        }
        return dumpable;
    }
}