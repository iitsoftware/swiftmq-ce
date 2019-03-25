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

import com.swiftmq.swiftlet.Swiftlet;

/**
 * LogSwiftlet to log information-, warning-, or error messages.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2012, All Rights Reserved
 */
public abstract class LogSwiftlet extends Swiftlet {
    /**
     * Log an information message
     *
     * @param source  usually the Swiftlet name
     * @param message the message to log
     */
    public abstract void logInformation(String source, String message);

    /**
     * Log a warning message
     *
     * @param source  usually the Swiftlet name
     * @param message the message to log
     */
    public abstract void logWarning(String source, String message);

    /**
     * Log an error message
     *
     * @param source  usually the Swiftlet name
     * @param message the message to log
     */
    public abstract void logError(String source, String message);

    /**
     * Creates a log sink under a specific name.
     *
     * @param name log sink name
     * @return log sink
     */
    public abstract LogSink createLogSink(String name);
}

