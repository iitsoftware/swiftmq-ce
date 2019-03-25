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
 * The ConnectionManager is responsible for registering and managing connections.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface ConnectionManager {
    /**
     * Returns the number of connections
     *
     * @return number of connections.
     */
    public int getNumberConnections();

    /**
     * Adds a connection.
     *
     * @param connection connection.
     */
    public void addConnection(Connection connection);

    /**
     * Removes and closes the connection.
     * This closes the connection asynchrounsly.
     *
     * @param connection connection.
     */
    public void removeConnection(Connection connection);


    /**
     * Removes and closes all connections.
     */
    public void removeAllConnections();
}

