/*
 * Copyright 2024 IIT Software GmbH
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

package com.swiftmq.impl.queue.standard.command;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.mgmt.Command;
import com.swiftmq.mgmt.CommandExecutor;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.TreeCommands;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueueReceiver;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.DataStreamOutputStream;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.Dom4JDriver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.text.DecimalFormat;

public class Exporter
        implements CommandExecutor {
    static final String COMMAND = "export";
    static final String PATTERN = "export <queuename> <routerdir> [-delete] [-xml] [-selector <selector>]";
    static final String DESCRIPTION = "Export Messages";
    static final DecimalFormat FMT = new DecimalFormat("0000000000");
    SwiftletContext ctx = null;

    public Exporter(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public Command createCommand() {
        return new Command(COMMAND, PATTERN, DESCRIPTION, true, this, true, true);
    }

    private MessageImpl copyMessage(MessageImpl msg) throws Exception {
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        DataByteArrayInputStream dbis = new DataByteArrayInputStream();
        msg.writeContent(dbos);
        dbis.reset();
        dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
        MessageImpl msgCopy = MessageImpl.createInstance(dbis.readInt());
        msgCopy.readContent(dbis);
        return msgCopy;
    }

    private void store(String queueName, XStream xStream, File outputDir, int nMsgs, MessageImpl message) throws Exception {
        String fn = "swiftmq_" + queueName + "_" + FMT.format(nMsgs);
        if (xStream != null) {
            File outFile = new File(outputDir, fn + ".xml");
            BufferedWriter bos = new BufferedWriter(new FileWriter(outFile));
            message.unfoldBuffers();
            xStream.toXML(message, bos);
            bos.flush();
            bos.close();
        } else {
            File outFile = new File(outputDir, fn + ".message");
            DataStreamOutputStream dos = new DataStreamOutputStream(new FileOutputStream(outFile));
            message.writeContent(dos);
            dos.flush();
            dos.close();
        }
    }

    public String[] execute(String[] context, Entity entity, String[] cmd) {
        if (cmd.length < 3)
            return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + TreeCommands.EXPORT + " <queuename> <localdir> [-remove] [-xml] [-selector <selector>]"};
        int nMsgs = 0;
        try {
            String queueName = cmd[1];
            String localDir = cmd[2];
            boolean delete = false;
            boolean xml = false;
            String selector = null;
            if (cmd.length > 3) {
                StringBuffer selBuffer = null;
                for (int i = 3; i < cmd.length; i++) {
                    if (selBuffer != null) {
                        if (selBuffer.length() > 0)
                            selBuffer.append(' ');
                        selBuffer.append(cmd[i]);
                    } else if (cmd[i].equals("-delete"))
                        delete = true;
                    else if (cmd[i].equals("-xml"))
                        xml = true;
                    else if (cmd[i].equals("-selector"))
                        selBuffer = new StringBuffer();
                    else
                        throw new Exception("Invalid option: " + cmd[i]);
                }
                if (selBuffer != null)
                    selector = selBuffer.toString();
            }

            XStream xStream = null;
            File outputDir = new File(localDir);
            if (!outputDir.exists()) {
                if (!outputDir.mkdir())
                    throw new Exception("Unable to create output directory: " + localDir);
            }
            if (xml) {
                xStream = new XStream(new Dom4JDriver());
                xStream.allowTypesByWildcard(new String[]{".*"});
            }
            MessageSelector msel = null;
            if (selector != null) {
                msel = new MessageSelector(selector);
                msel.compile();
            }
            QueueReceiver receiver = ctx.queueManager.createQueueReceiver(queueName, null, msel);
            QueuePullTransaction pullTx = receiver.createTransaction(false);
            try {
                MessageEntry entry = null;
                while ((entry = pullTx.getMessage(0, msel)) != null) {
                    MessageImpl msg = delete ? entry.getMessage() : copyMessage(entry.getMessage());
                    msg.clearSwiftMQAllProps();
                    store(queueName, xStream, outputDir, nMsgs++, msg);
                    if (delete) {
                        pullTx.commit();
                        pullTx = receiver.createTransaction(false);
                    }
                }
            } finally {
                try {
                    pullTx.rollback();
                } catch (Exception e) {
                }
                try {
                    receiver.close();
                } catch (Exception e) {
                }
            }
        } catch (Exception e) {
            return new String[]{TreeCommands.ERROR, e.getMessage()};
        }
        return new String[]{TreeCommands.INFO, nMsgs + " messages exported."};
    }
}