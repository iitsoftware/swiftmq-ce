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

package com.swiftmq.impl.amqp.sasl.v01_00_00;

import com.swiftmq.amqp.ProtocolHeader;
import com.swiftmq.impl.amqp.Handler;
import com.swiftmq.impl.amqp.HandlerFactory;
import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.VersionedConnection;

public class SASLHandlerFactory implements HandlerFactory {
    public static final ProtocolHeader SASL_INIT = new ProtocolHeader("AMQP", 3, 1, 0, 0);

    SwiftletContext ctx = null;

    public SASLHandlerFactory(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public Handler createHandler(VersionedConnection versionedConnection) {
        return new SASLHandler(ctx, versionedConnection);
    }
}
