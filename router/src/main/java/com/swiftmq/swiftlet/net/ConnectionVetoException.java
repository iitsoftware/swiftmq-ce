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

/**
 * A ConnectionVetoException must be thrown by a <code>ConnectionListener</code>
 * when a connection could not be accepted for any Swiftlet specific reasons, e.g.
 * not authorized.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see com.swiftmq.swiftlet.net.event.ConnectionListener
 */
public class ConnectionVetoException extends Exception {

    /**
     * Constructs a new ConnectionVetoException.
     *
     * @param message exception message.
     */
    public ConnectionVetoException(String message) {
        super(message);
    }
}

