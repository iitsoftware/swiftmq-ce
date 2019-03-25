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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SendDirConcurrently {
    static Connection connection = null;
    static Queue linkInputQueue = null;
    static String routerName = null;
    static String cacheName = null;

    private static void transfer(File file) throws Exception {
        if (file.length() == 0)
            return;
        System.out.println("Transferring: " + file.getName());
        Filetransfer filetransfer = Filetransfer.create(connection, routerName, cacheName).withDigestType("MD5");
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("orderid", new Random().nextInt());
        props.put("customerid", new Random().nextInt());
        boolean isPrivate = file.getName().contains("_eval");
        String link = filetransfer.withFile(file).withFileIsPrivate(isPrivate).withPassword("Cheers").withProperties(props).send(new ProgressListener() {
            public void progress(String filename, int chunksTransferred, long fileSize, long bytesTransferred, int transferredPercent) {
                System.out.println("  " + filename + ": " + chunksTransferred + " chunks, " + bytesTransferred + " of " + fileSize + " transferred (" + transferredPercent + "%)");
            }
        });
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer linkSender = session.createProducer(linkInputQueue);
        linkSender.send(session.createTextMessage(link));
        session.close();
        filetransfer.close();
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("Usage: SendDirConcurrently <smqp-url> <connection-factory> <filename> <routerName> <cacheName>");
            System.exit(-1);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        try {
            File file = new File(args[2]);
            if (!file.exists())
                throw new Exception("File '" + args[2] + "' does not exists!");

            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.swiftmq.jndi.InitialContextFactoryImpl");
            env.put(Context.PROVIDER_URL, args[0]);
            InitialContext ctx = new InitialContext(env);
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(args[1]);
            linkInputQueue = (Queue) ctx.lookup("link-input");
            ctx.close();
            routerName = args[3];
            cacheName = args[4];
            connection = cf.createConnection();
            connection.start();
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null && files.length > 0) {
                    final CountDownLatch countDownLatch = new CountDownLatch(files.length);
                    for (int i = 0; i < files.length; i++)
                        if (!files[i].isDirectory()) {
                            final File f = files[i];
                            executorService.execute(new Runnable() {
                                public void run() {
                                    try {
                                        transfer(f);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    countDownLatch.countDown();
                                }
                            });
                        } else
                            countDownLatch.countDown();
                    countDownLatch.await();
                }
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
