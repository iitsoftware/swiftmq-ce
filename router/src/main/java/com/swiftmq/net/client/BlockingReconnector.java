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

package com.swiftmq.net.client;

import com.swiftmq.jms.SwiftMQConnectionFactory;
import com.swiftmq.net.SocketFactory;

import java.net.Socket;
import java.util.List;
import java.util.Map;

public class BlockingReconnector extends Reconnector {
    public BlockingReconnector(List servers, Map parameters, boolean enabled, int maxRetries, long retryDelay, boolean debug) {
        super(servers, parameters, enabled, maxRetries, retryDelay, debug);
    }

    protected Connection createConnection(ServerEntry entry, Map parameters) {
        Connection connection = null;
        try {
            boolean tcpNoDelay = ((Boolean) parameters.get(SwiftMQConnectionFactory.TCP_NO_DELAY)).booleanValue();
            int inputBufferSize = ((Integer) parameters.get(SwiftMQConnectionFactory.INPUT_BUFFER_SIZE)).intValue();
            int inputExtendSize = ((Integer) parameters.get(SwiftMQConnectionFactory.INPUT_EXTEND_SIZE)).intValue();
            int outputBufferSize = ((Integer) parameters.get(SwiftMQConnectionFactory.OUTPUT_BUFFER_SIZE)).intValue();
            int outputExtendSize = ((Integer) parameters.get(SwiftMQConnectionFactory.OUTPUT_EXTEND_SIZE)).intValue();
            SocketFactory socketFactory = (SocketFactory) parameters.get(SwiftMQConnectionFactory.SOCKETFACTORY);
            Socket socket = socketFactory.createSocket(entry.getHostname(), entry.getPort(), tcpNoDelay);
            connection = new BlockingConnection(socket, inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize);
        } catch (Exception e) {
            if (debug) System.out.println(toString() + " exception creating connection: " + e);
        }
        return connection;
    }

    public String toString() {
        return "[BlockingReconnector, servers=" + servers + "]";
    }
}
