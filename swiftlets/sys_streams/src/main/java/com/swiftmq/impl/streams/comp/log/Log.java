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

package com.swiftmq.impl.streams.comp.log;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.swiftlet.log.LogSink;

/**
 * Represents the Stream's log file. The log file is maintained by the Log Swiftlet
 * and located in the log sink directory.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class Log {
    StreamContext ctx;
    LogSink logSink;

    /**
     * Internal use.
     */
    public Log(StreamContext ctx) {
        this.ctx = ctx;
        logSink = ctx.ctx.logSwiftlet.createLogSink("stream_" + ctx.stream.fullyQualifiedName());
    }

    /**
     * Write an informational message to the log file.
     *
     * @param message message
     * @return Log
     */
    public Log info(String message) {
        logSink.log("INFORMATION/" + message);
        return this;
    }

    /**
     * Write a warning message to the log file.
     *
     * @param message message
     * @return Log
     */
    public Log warning(String message) {
        logSink.log("WARNING/" + message);
        return this;
    }

    /**
     * Write a error message to the log file.
     *
     * @param message message
     * @return Log
     */
    public Log error(String message) {
        logSink.log("ERROR/" + message);
        return this;
    }

    /**
     * Internal use.
     */
    public void close() {
        logSink.close();
    }

}
