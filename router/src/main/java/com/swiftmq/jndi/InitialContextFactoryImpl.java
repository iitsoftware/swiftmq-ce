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

package com.swiftmq.jndi;

import com.swiftmq.jndi.v400.ContextImpl;
import com.swiftmq.jndi.v400.JNDIInfo;
import com.swiftmq.jndi.v400.URLParser;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class InitialContextFactoryImpl implements InitialContextFactory, java.io.Serializable {
    public static final String RECONNECT = "swiftmq.jndi.reconnect";
    public static final String RECONNECT_DEBUG = "swiftmq.jndi.reconnect.debug";
    public static final String MAX_RETRIES = "swiftmq.jndi.reconnect.max.retries";
    public static final String RETRY_DELAY = "swiftmq.jndi.reconnect.retry.delay";

    public Context getInitialContext(Hashtable env)
            throws NamingException {
        String s = (String) env.get(RECONNECT);
        if (s != null && Boolean.valueOf(s).booleanValue()) {
            int maxRetries = 5;
            long retryDelay = 2000;
            s = (String) env.get(MAX_RETRIES);
            if (s != null)
                maxRetries = Integer.parseInt(s);
            s = (String) env.get(RETRY_DELAY);
            if (s != null)
                retryDelay = Long.parseLong(s);
            List envList = new ArrayList();
            Hashtable ht = new Hashtable();
            ht.put(Context.PROVIDER_URL, env.get(Context.PROVIDER_URL));
            ht.put(Context.INITIAL_CONTEXT_FACTORY, env.get(Context.INITIAL_CONTEXT_FACTORY));
            envList.add(ht);
            int i = 2;
            boolean found = true;
            while (found) {
                s = (String) env.get(Context.PROVIDER_URL + "_" + i);
                if (s == null)
                    found = false;
                else {
                    ht = new Hashtable();
                    ht.put(Context.PROVIDER_URL, s);
                    ht.put(Context.INITIAL_CONTEXT_FACTORY, env.get(Context.INITIAL_CONTEXT_FACTORY));
                    envList.add(ht);
                    i++;
                }
            }
            ReconnectContext rc = new ReconnectContext(envList, maxRetries, retryDelay);
            s = (String) env.get(RECONNECT_DEBUG);
            rc.setDebug(s != null && Boolean.valueOf(s).booleanValue());
            return rc;
        } else {
            JNDIInfo jndiInfo = URLParser.parseURL((String) env.get(Context.PROVIDER_URL));
            if (jndiInfo.isReconnect()) {
                List envList = new ArrayList();
                Hashtable env1 = new Hashtable();
                env1.put(Context.PROVIDER_URL, jndiInfo.getProviderURL(jndiInfo.getHostname(), jndiInfo.getPort()));
                env1.put(Context.INITIAL_CONTEXT_FACTORY, env.get(Context.INITIAL_CONTEXT_FACTORY));
                envList.add(env1);
                if (jndiInfo.getHostname2() != null) {
                    Hashtable env2 = new Hashtable();
                    env2.put(Context.PROVIDER_URL, jndiInfo.getProviderURL(jndiInfo.getHostname2(), jndiInfo.getPort2()));
                    env2.put(Context.INITIAL_CONTEXT_FACTORY, env.get(Context.INITIAL_CONTEXT_FACTORY));
                    envList.add(env2);
                }
                ReconnectContext rc = new ReconnectContext(envList, jndiInfo.getMaxRetries(), jndiInfo.getReconnectDelay());
                rc.setDebug(jndiInfo.isDebug());
                return rc;
            } else
                return new ContextImpl(env);
        }
    }
}

