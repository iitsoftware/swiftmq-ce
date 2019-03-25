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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Hashtable;

public class QueryProperties {
    static Connection connection = null;

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: QueryProperties <smqp-url> <connection-factory> <routername> <cachename> [<link>]");
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

            Filetransfer filetransfer = Filetransfer.create(connection, args[2], args[3]).withSelector(null);
            if (args.length == 5)
                System.out.println(filetransfer.withLink(args[4]).queryProperties());
            else
                System.out.println(filetransfer.withSelector("JMS_SWIFTMQ_FT_FILENAME LIKE '%.properties'").queryProperties());
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
