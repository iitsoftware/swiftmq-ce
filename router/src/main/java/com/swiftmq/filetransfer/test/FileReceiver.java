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
import java.util.Hashtable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileReceiver {
    static Connection connection = null;
    static File outDir = null;
    static ExecutorService executorService = Executors.newFixedThreadPool(5);

    private static void transfer(String link) throws Exception {
        System.out.println("Transferring: " + link);
        Filetransfer filetransfer = Filetransfer.create(connection, link).withDigestType("MD5");
        filetransfer.withOriginalFilename(true).withOutputDirectory(outDir).withPassword("Moin!").receive(new ProgressListener() {
            public void progress(String filename, int chunksTransferred, long fileSize, long bytesTransferred, int transferredPercent) {
                System.out.println("  " + filename + ": " + chunksTransferred + " chunks, " + bytesTransferred + " of " + fileSize + " transferred (" + transferredPercent + "%)");
            }
        }).delete().close();
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: FileReceiver <smqp-url> <connection-factory> <link-input-dest> <output-dir>");
            System.exit(-1);
        }

        try {
            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.swiftmq.jndi.InitialContextFactoryImpl");
            env.put(Context.PROVIDER_URL, args[0]);
            InitialContext ctx = new InitialContext(env);
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(args[1]);
            Destination linkInputDest = (Destination) ctx.lookup(args[2]);
            ctx.close();
            connection = cf.createConnection();
            connection.start();
            outDir = new File(args[3]);
            if (!outDir.exists())
                outDir.mkdirs();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(linkInputDest);
            TextMessage msg = null;
            while ((msg = (TextMessage) consumer.receive()) != null) {
                final String link = msg.getText();
                executorService.execute(new Runnable() {
                    public void run() {
                        try {
                            transfer(link);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
            try {
                connection.close();
            } catch (JMSException e) {
            }
        }
    }

}
