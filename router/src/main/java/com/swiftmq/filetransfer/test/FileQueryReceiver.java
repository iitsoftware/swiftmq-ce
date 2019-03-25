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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.File;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileQueryReceiver {
    static Connection connection = null;
    static File outDir = null;
    static ExecutorService executorService = Executors.newFixedThreadPool(5);
    static CountDownLatch countDownLatch = null;

    private static void transfer(String link) throws Exception {
        System.out.println("Transferring: " + link);
        Filetransfer filetransfer = Filetransfer.create(connection, link).withDigestType("MD5");
        filetransfer.withOriginalFilename(true).withOutputDirectory(outDir).withPassword("Cheers").receive(new ProgressListener() {
            public void progress(String filename, int chunksTransferred, long fileSize, long bytesTransferred, int transferredPercent) {
                System.out.println("  " + filename + ": " + chunksTransferred + " chunks, " + bytesTransferred + " of " + fileSize + " transferred (" + transferredPercent + "%)");
            }
        }).delete().close();
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("Usage: FileReceiver <smqp-url> <connection-factory> <routername> <cachename> <output-dir>");
            System.exit(-1);
        }

        try {
            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.swiftmq.jndi.InitialContextFactoryImpl");
            env.put(Context.PROVIDER_URL, args[0]);
            InitialContext ctx = new InitialContext(env);
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup(args[1]);
            ctx.close();
            connection = cf.createConnection();
            connection.start();
            outDir = new File(args[4]);
            if (!outDir.exists())
                outDir.mkdirs();

            Filetransfer filetransfer = Filetransfer.create(connection, args[2], args[3]).withSelector(null);
            List<String> result = filetransfer.query();
            if (result != null && result.size() > 0) {
                countDownLatch = new CountDownLatch(result.size());
                for (int i = 0; i < result.size(); i++) {
                    final String link = result.get(i);
                    executorService.execute(new Runnable() {
                        public void run() {
                            try {
                                transfer(link);
                                countDownLatch.countDown();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
            }
            filetransfer.close();
            if (countDownLatch != null)
                countDownLatch.await();
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
