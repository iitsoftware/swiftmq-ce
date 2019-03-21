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

package com.swiftmq.impl.routing.single.smqpr.v400;

import com.swiftmq.tools.dump.*;

public class SMQRFactory extends DumpableFactory
{
  // Id must start with 100

  // Connect handshake
  public static final int CONNECT_REQ = 101;
  public static final int CONNECT_REPREQ = 102;
  public static final int AUTH_REQ = 103;
  public static final int AUTH_REPREQ = 104;

  // Delivery handshake
  public static final int ADJUST_REQ = 110;
  public static final int STARTDELIVERY_REQ = 111;

  // 2PC
  public static final int RECOVERY_REQ = 120;
  public static final int RECOVERY_REPREQ = 121;
  public static final int TRANSACTION_REQ = 122;
  public static final int COMMIT_REQ = 123;
  public static final int COMMIT_REPREQ = 124;
  public static final int ROLLBACK_REQ = 125;
  public static final int ROLLBACK_REPREQ = 126;

  // Route exchange
  public static final int ROUTE_REQ = 127;

  // Throttling
  public static final int THROTTLE_REQ = 128;

  public Dumpable createDumpable(int dumpId)
  {
    Dumpable dumpable = null;

    switch (dumpId)
    {
      case CONNECT_REQ:
        dumpable = new ConnectRequest();
        break;
      case CONNECT_REPREQ:
        dumpable = new ConnectReplyRequest();
        break;
      case AUTH_REQ:
        dumpable = new AuthRequest();
        break;
      case AUTH_REPREQ:
        dumpable = new AuthReplyRequest();
        break;
      case ADJUST_REQ:
        dumpable = new AdjustRequest();
        break;
      case STARTDELIVERY_REQ:
        dumpable = new StartDeliveryRequest();
        break;
      case RECOVERY_REQ:
        dumpable = new RecoveryRequest();
        break;
      case RECOVERY_REPREQ:
        dumpable = new RecoveryReplyRequest();
        break;
      case TRANSACTION_REQ:
        dumpable = new TransactionRequest();
        break;
      case COMMIT_REQ:
        dumpable = new CommitRequest();
        break;
      case COMMIT_REPREQ:
        dumpable = new CommitReplyRequest();
        break;
      case ROLLBACK_REQ:
        dumpable = new RollbackRequest();
        break;
      case ROLLBACK_REPREQ:
        dumpable = new RollbackReplyRequest();
        break;
      case ROUTE_REQ:
        dumpable = new RouteRequest();
        break;
    }
    return dumpable;
  }
}
