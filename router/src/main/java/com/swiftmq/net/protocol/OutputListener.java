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

package com.swiftmq.net.protocol;

import java.io.IOException;

/**
 * An OutputListener is responsible to write bytes to the network. It is set
 * at a ProtocolOutputHandler and implemented by the Network Swiftlet.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see ProtocolOutputHandler
 */
public interface OutputListener {

    /**
     * Performs the write to the network.
     *
     * @param b      byte array.
     * @param offset offset.
     * @param len    length.
     * @return number of bytes written.
     * @throws IOException on error.
     */
    public int performWrite(byte[] b, int offset, int len)
            throws IOException;
}

