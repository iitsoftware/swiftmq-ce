/*
 * Copyright 2023 IIT Software GmbH
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

package com.swiftmq.impl.store.standard.log;

import java.io.RandomAccessFile;

public interface LogManager {
    String FILENAME = "transaction.log";
    String TP_LM = "sys$store.logmanager";
    String PROP_VERBOSE = "swiftmq.store.checkpoint.verbose";
    boolean checkPointVerbose = Boolean.getBoolean(PROP_VERBOSE);

    void setLogManagerListener(LogManagerListener logManagerListener);

    RandomAccessFile getLogFile();

    void setForceSync(boolean forceSync);

    boolean isForceSync();

    void initLogFile();
}
