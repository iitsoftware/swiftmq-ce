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

package com.swiftmq.swiftlet.net;

import com.swiftmq.net.protocol.ProtocolInputHandler;
import com.swiftmq.net.protocol.ProtocolOutputHandler;
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.net.event.ConnectionListener;

/**
 * A IntraVMListenerMetaData object describes an intra-VM listener.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class IntraVMListenerMetaData extends ConnectionMetaData {
    /**
     * Constructs a new IntraVMListenerMetaData with default SMQP protocol handlers.
     *
     * @param swiftlet           Swiftlet.
     * @param connectionListener connection listener.
     */
    public IntraVMListenerMetaData(Swiftlet swiftlet, ConnectionListener connectionListener) {
        super(swiftlet, 0, null, connectionListener, 0, 0, 0, 0, true);
    }

    /**
     * Constructs a new ConnectionMetaData with custom protocol handlers.
     *
     * @param swiftlet              Swiftlet.
     * @param connectionListener    connection listener.
     * @param protocolInputHandler  protocol input handler.
     * @param protocolOutputHandler protocol output handler.
     */
    public IntraVMListenerMetaData(Swiftlet swiftlet, ConnectionListener connectionListener,
                                   ProtocolInputHandler protocolInputHandler, ProtocolOutputHandler protocolOutputHandler) {
        super(swiftlet, 0, null, connectionListener, 0, 0, 0, 0, true, protocolInputHandler, protocolOutputHandler);
    }

    public String toString() {
        return "[IntraVMListenerMetaData, " + super.toString() + "]";
    }
}
