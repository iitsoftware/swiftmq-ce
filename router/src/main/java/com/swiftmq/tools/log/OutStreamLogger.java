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

package com.swiftmq.tools.log;

import java.io.OutputStream;
import java.io.PrintWriter;

/**
 * Implementation eines Loggers, der den Log-Output auf einen OutputStream schreibt
 *
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class OutStreamLogger extends Logger {
    PrintWriter writer = null;

    /**
     * Erzeugt einen OutstreamLogger mit einem Prefix und dem OutputStream
     *
     * @param prefix
     * @param outstream
     */
    public OutStreamLogger(String prefix, OutputStream outstream) {
        super(prefix);
        writer = new PrintWriter(outstream);
    }

    /**
     * Loggt das LogValue
     *
     * @param logValue The log value
     */
    public void logMessage(LogValue logValue) {
        writer.println(prefix + ": " + logValue);
        writer.flush();
    }
}

