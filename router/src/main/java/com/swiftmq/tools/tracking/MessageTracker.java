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

package com.swiftmq.tools.tracking;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.util.SwiftUtilities;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MessageTracker {
    private static final SimpleDateFormat format = new SimpleDateFormat("yyMMdd:HH:mm:ss.SSS");
    public static boolean enabled = Boolean.valueOf(System.getProperty("swiftmq.message.tracking.enabled", "false")).booleanValue();
    private static String filename = System.getProperty("swiftmq.message.tracking.filename", "track.out");
    private static String sequenceProp = System.getProperty("swiftmq.message.tracking.sequenceprop");
    private static long rolloverSize = (long) (Integer.parseInt(System.getProperty("swiftmq.message.tracking.rolloversize", "10240")) * 1024);
    private static int keepFiles = Integer.parseInt(System.getProperty("swiftmq.message.tracking.keepfiles", "10"));
    String prefix = null;
    List files = new ArrayList();
    File current = null;
    PrintWriter writer = null;
    int idx = 0;

    private MessageTracker() {
    }

    private static class InstanceHolder {
        public static MessageTracker instance = new MessageTracker();
    }

    public static MessageTracker getInstance() {
        return InstanceHolder.instance;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    private File newFile(int n) {
        if (files.size() == keepFiles) {
            File f = (File) files.remove(0);
            f.delete();
        }
        File file = new File(n + "_" + filename);
        files.add(file);
        return file;
    }

    public synchronized void track(MessageImpl msg, String[] callStack, String operation) {
        try {
            if (current == null) {
                current = newFile(idx++);
                writer = new PrintWriter(new BufferedWriter(new FileWriter(current)));
            }
            StringBuffer b = new StringBuffer(format.format(new Date()));
            if (prefix != null) {
                b.append("-");
                b.append(prefix);
            }
            if (sequenceProp != null && msg.propertyExists(sequenceProp)) {
                b.append("-");
                b.append(SwiftUtilities.fillLeft(msg.getObjectProperty(sequenceProp).toString(), 6, '0'));
            }
            b.append("-");
            b.append(msg.getJMSDestination());
            if (msg.getJMSMessageID() != null) {
                b.append("-");
                b.append(msg.getJMSMessageID());
            }
            b.append("-");
            for (int i = 0; i < callStack.length; i++) {
                if (i > 0)
                    b.append("/");
                b.append(callStack[i]);
            }
            b.append("-");
            b.append(operation);
            writer.println(b.toString());
            writer.flush();
            if (current.length() >= rolloverSize) {
                writer.close();
                current = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
