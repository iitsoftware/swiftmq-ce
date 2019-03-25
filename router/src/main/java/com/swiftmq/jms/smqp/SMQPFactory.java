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

package com.swiftmq.jms.smqp;

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;

public class SMQPFactory extends DumpableFactory {
    public static final int DID_SMQP_VERSION_REQ = 0;
    public static final int DID_SMQP_VERSION_REP = 1;

    DumpableFactory protocolFactory = null;

    public SMQPFactory(DumpableFactory protocolFactory) {
        this.protocolFactory = protocolFactory;
    }

    public SMQPFactory() {
        this(null);
    }

    public void setProtocolFactory(DumpableFactory protocolFactory) {
        this.protocolFactory = protocolFactory;
    }

    public Dumpable createDumpable(int dumpId) {
        Dumpable d = null;
        switch (dumpId) {
            case DID_SMQP_VERSION_REQ:
                d = new SMQPVersionRequest();
                break;
            case DID_SMQP_VERSION_REP:
                d = new SMQPVersionReply();
                break;
            default:
                if (protocolFactory != null)
                    d = protocolFactory.createDumpable(dumpId);
                break;
        }
        return d;
    }
}
