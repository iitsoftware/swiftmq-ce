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

import java.io.IOException;
import java.io.InputStream;

/**
 * An InboundHandler should be registered at a <code>Connection</code> from a
 * <code>ConnectionListener</code> during the <code>connected()</code> call. It will
 * be informed when data is available on the connection's input stream and should
 * read this data out of the stream.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see Connection
 * @see com.swiftmq.swiftlet.net.event.ConnectionListener
 */
public interface InboundHandler {

    /**
     * Will be called if data is available on the connection's input stream.
     * The inbound handler must read this data out of the stream and should
     * forward it to further processing to some async running task to return
     * immediately. If this method throws an IOException, the connection will
     * be closed.
     *
     * @param connection  connection.
     * @param inputStream the connections's input stream.
     * @throws IOException on error to close the connection.
     */
    public void dataAvailable(Connection connection, InputStream inputStream)
            throws IOException;
}

