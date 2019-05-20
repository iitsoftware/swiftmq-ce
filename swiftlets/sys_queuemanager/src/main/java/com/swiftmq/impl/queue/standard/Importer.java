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

package com.swiftmq.impl.queue.standard;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.mgmt.Command;
import com.swiftmq.mgmt.CommandExecutor;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.TreeCommands;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;
import com.swiftmq.tools.util.DataStreamInputStream;
import com.swiftmq.tools.util.IdGenerator;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.Dom4JDriver;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Arrays;

public class Importer
        implements CommandExecutor {
    static final String COMMAND = "import";
    static final String PATTERN = "import <routerdir> <queuename> [-newid] [-delete] [-filter <regex>]";
    static final String DESCRIPTION = "Import Messages";
    static final DecimalFormat FMT = new DecimalFormat("0000000000");
    SwiftletContext ctx = null;

    public Importer(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public Command createCommand() {
        return new Command(COMMAND, PATTERN, DESCRIPTION, true, this, true, true);
    }

    public String[] execute(String[] context, Entity entity, String[] cmd) {
        if (cmd.length < 3)
            return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + TreeCommands.IMPORT + " <localdir> <queuename> [-newheader] [-delete] [-filter <regex>]"};
        int nMsgs = 0;
        try {
            String localDir = cmd[1];
            String queueName = cmd[2];
            boolean delete = false;
            boolean newId = false;
            String filter = null;
            if (cmd.length > 3) {
                StringBuffer selBuffer = null;
                for (int i = 3; i < cmd.length; i++) {
                    if (selBuffer != null)
                        selBuffer.append(cmd[i]);
                    else if (cmd[i].equals("-delete"))
                        delete = true;
                    else if (cmd[i].equals("-newid"))
                        newId = true;
                    else if (cmd[i].equals("-filter"))
                        selBuffer = new StringBuffer();
                    else
                        throw new Exception("Invalid option: " + cmd[i]);
                }
                if (selBuffer != null)
                    filter = selBuffer.toString();
            }

            String idPrefix = null;
            if (newId) {
                StringBuffer b = new StringBuffer(SwiftletManager.getInstance().getRouterName());
                b.append('/');
                b.append(IdGenerator.getInstance().nextId('/'));
                b.append('/');
                idPrefix = b.toString();
            }

            XStream xStream = null;
            File inputDir = new File(localDir);
            if (!inputDir.exists())
                throw new Exception("Input directory doesn't exists: " + localDir);

            xStream = new XStream(new Dom4JDriver());
            XStream.setupDefaultSecurity(xStream);

            QueueSender sender = ctx.queueManager.createQueueSender(queueName, null);
            try {
                MessageImpl msg = null;
                File[] files = null;
                files = inputDir.listFiles(new RegexFilter(filter) {
                    public boolean accept(File file, String s) {
                        return regex == null || s.matches(regex);
                    }
                });
                if (files != null && files.length > 0) {
                    Arrays.sort(files);
                    for (int i = 0; i < files.length; i++) {
                        msg = null;
                        if (!files[i].isDirectory()) {
                            if (files[i].getName().endsWith(".xml")) {
                                BufferedReader bufferedReader = new BufferedReader(new FileReader(files[i]));
                                msg = (MessageImpl) xStream.fromXML(bufferedReader);
                                bufferedReader.close();
                            } else if (files[i].getName().endsWith(".message")) {
                                DataStreamInputStream dis = new DataStreamInputStream(new BufferedInputStream(new FileInputStream(files[i])));
                                msg = MessageImpl.createInstance(dis.readInt());
                                msg.readContent(dis);
                                dis.close();
                            }
                            if (msg != null) {
                                if (newId) {
                                    StringBuffer b = new StringBuffer(idPrefix);
                                    b.append(nMsgs);
                                    msg.setJMSMessageID(b.toString());
                                }
                                QueuePushTransaction pushTx = sender.createTransaction();
                                pushTx.putMessage(msg);
                                pushTx.commit();
                                if (delete)
                                    files[i].delete();
                                nMsgs++;
                            }
                        }
                    }
                }
            } finally {
                try {
                    sender.close();
                } catch (Exception e) {
                }
            }
        } catch (Exception e) {
            return new String[]{TreeCommands.ERROR, e.getMessage()};
        }
        return new String[]{TreeCommands.INFO, nMsgs + " messages imported."};
    }

    private abstract class RegexFilter implements FilenameFilter {
        String regex = null;

        protected RegexFilter(String regex) {
            this.regex = regex;
        }
    }
}