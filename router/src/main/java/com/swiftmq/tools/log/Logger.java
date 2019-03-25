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

/**
 * Abstrakte Klasse, die Log implementiert und ein Prefix speichert, das dann als
 * Prefix fuer logMessages genommen werden kann.
 *
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public abstract class Logger implements Log {
    String prefix = null;

    /**
     * Erzeugt einen neuen Logger mit einem Prefix
     *
     * @param prefix
     * @SBGen Constructor assigns prefix
     */
    public Logger(String prefix) {
        // SBgen: Assign variable
        this.prefix = prefix;
    }

    /**
     * Loggen eines LogValues
     *
     * @param logValue The log value
     */
    public abstract void logMessage(LogValue logValue);
}

