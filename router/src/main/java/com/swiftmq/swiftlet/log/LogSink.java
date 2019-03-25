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

package com.swiftmq.swiftlet.log;

/**
 * A LogSink is a custom destination for log messages.
 *
 * @author IIT Software GmbH, Bremen/Germany, Copyright (c) 2000-2012, All Rights Reserved
 */

public abstract class LogSink {
    public boolean enabled = true;

    /**
     * Send a message to the log sink.
     *
     * @param message Message
     */
    public abstract void log(String message);

    /**
     * Closes the log sink.
     */
    public abstract void close();
}
