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

package com.swiftmq.filetransfer.test;

import com.swiftmq.filetransfer.Filetransfer;
import com.swiftmq.filetransfer.ProgressListener;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class SendFile {
    static Filetransfer filetransfer = null;
    static Session session = null;
    static MessageProducer linkSender = null;

    private static void transfer(File file) throws Exception {
        if (file.length() == 0)
            return;
        System.out.println("Transferring: " + file.getName());
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("path", file.getPath());
        props.put("absolutepath", file.getAbsolutePath());
        props.put("canonicalpath", file.getCanonicalPath());
        String link = filetransfer.withFile(file).withProperties(props).withPassword("Moin!").send(new ProgressListener() {
            public void progress(String filename, int chunksTransferred, long fileSize, long bytesTransferred, int transferredPercent) {
                System.out.println("  " + filename + ": " + chunksTransferred + " chunks, " + bytesTransferred + " of " + fileSize + " transferred (" + transferredPercent + "%)");
            }
        });
        linkSender.send(session.createTextMessage(link));
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("Usage: SendDirConcurrently <smqp-url> <connection-factory> <filename> <routerName> <cacheName>");
            System.exit(-1);
        }
        Connection connection = null;
        try {
            File file = new File(args[2]);
            if (!file.exists())
                throw new Exception("File '" + args[2] + "' does not exists!");

            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.swiftmq.jndi.InitialContextFactoryImpl");
            env.put(Context.PROVIDER_URL, args[0]);
            InitialContext ctx = new InitialContext(env);
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(args[1]);
            Destination linkInputQueue = (Queue) ctx.lookup("link-input");
            ctx.close();
            connection = cf.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            linkSender = session.createProducer(linkInputQueue);
            connection.start();

            filetransfer = Filetransfer.create(connection, args[3], args[4]).withDigestType("MD5");
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null && files.length > 0) {
                    for (int i = 0; i < files.length; i++)
                        if (!files[i].isDirectory())
                            transfer(files[i]);
                }
            } else
                transfer(file);
            filetransfer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (JMSException e) {
            }
        }
    }

}
