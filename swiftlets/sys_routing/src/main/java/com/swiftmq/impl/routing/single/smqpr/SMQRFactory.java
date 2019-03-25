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

package com.swiftmq.impl.routing.single.smqpr;

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;

public class SMQRFactory extends DumpableFactory {
    public static final int MAX_DUMP_ID = 512;
    public static final int PROTOCOL_REQ = 0;
    public static final int PROTOCOL_REPREQ = 1;
    public static final int BULK_REQ = 2;
    public static final int KEEPALIVE_REQ = 3;
    public static final int START_STAGE_REQ = 4;
    public static final int CLOSE_STAGE_QUEUE_REQ = 5;
    public static final int DELIVERY_REQ = 6;
    public static final int SEND_ROUTE_REQ = 7;

    // Accounting
    public static final int START_ACCOUNTING_REQ = 8;
    public static final int FLUSH_ACCOUNTING_REQ = 9;
    public static final int STOP_ACCOUNTING_REQ = 10;

    DumpableFactory protocolFactory = null;
    BulkRequest bulkRequest = null;

    public SMQRFactory() {
        bulkRequest = new BulkRequest(this);
    }

    public void setProtocolFactory(DumpableFactory protocolFactory) {
        this.protocolFactory = protocolFactory;
    }

    public Dumpable createDumpable(int dumpId) {
        Dumpable dumpable = null;

        switch (dumpId) {
            case PROTOCOL_REQ:
                dumpable = new ProtocolRequest();
                break;
            case PROTOCOL_REPREQ:
                dumpable = new ProtocolReplyRequest();
                break;
            case BULK_REQ:
                dumpable = bulkRequest;
                break;
            case KEEPALIVE_REQ:
                dumpable = new KeepAliveRequest();
                break;
            case START_STAGE_REQ:
                dumpable = new StartStageRequest();
                break;
            case CLOSE_STAGE_QUEUE_REQ:
                dumpable = new CloseStageQueueRequest();
                break;
            case DELIVERY_REQ:
                dumpable = new DeliveryRequest();
                break;
            case SEND_ROUTE_REQ:
                dumpable = new SendRouteRequest();
                break;
            case START_ACCOUNTING_REQ:
                dumpable = new StartAccountingRequest();
                break;
            case FLUSH_ACCOUNTING_REQ:
                dumpable = new FlushAccountingRequest();
                break;
            case STOP_ACCOUNTING_REQ:
                dumpable = new StopAccountingRequest();
                break;
            default:
                if (protocolFactory != null)
                    dumpable = protocolFactory.createDumpable(dumpId);
                break;
        }
        return dumpable;
    }
}
