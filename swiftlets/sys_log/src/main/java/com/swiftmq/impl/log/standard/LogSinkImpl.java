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

package com.swiftmq.impl.log.standard;

import com.swiftmq.swiftlet.log.LogSink;

import java.io.PrintWriter;
import java.util.Calendar;

public class LogSinkImpl extends LogSink {
    PrintWriter printWriter = null;

    public LogSinkImpl(PrintWriter printWriter) {
        this.printWriter = printWriter;
    }

    private String createLogLine(String msg) {
        Calendar cal = Calendar.getInstance();
        StringBuffer outline = new StringBuffer();
        outline.append(LogSwiftletImpl.fmt.format(cal.getTime()));
        outline.append(msg);
        return outline.toString();
    }

    public void log(String message) {
        printWriter.println(createLogLine(message));
    }

    public void close() {
        printWriter.close();
    }
}
