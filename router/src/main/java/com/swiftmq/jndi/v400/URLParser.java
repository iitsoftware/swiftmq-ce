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

package com.swiftmq.jndi.v400;

import javax.naming.NamingException;
import java.util.StringTokenizer;

// Parser for SMQP-URLs:
// "smqp://[<user>[:<passwd>]@](<host>:<port>)|"intravm"[/[type=<type>][;timeout=<ms>][;keepalive=<ms>]
//           [;reconnect=<boolean>][;retrydelay=<ms>][;maxretries=<int>][;host2=<host>][;port2=<port>][;debug=<boolean>]]"

public class URLParser implements java.io.Serializable {
    private static final String DEFAULT_FACTORY = "com.swiftmq.net.PlainSocketFactory";
    private static final String INTRAVM = "intravm";
    private static final long DEFAULT_TIMEOUT = 0;
    private static final long DEFAULT_KEEPALIVE = 60000;

    private static String[] getPars(String url) {
        StringTokenizer t = new StringTokenizer(url, ";");
        String[] pars = new String[t.countTokens()];
        int i = 0;
        while (t.hasMoreTokens())
            pars[i++] = t.nextToken();
        return pars;
    }

    private static String getName(String par) throws NamingException {
        int idx = par.indexOf('=');
        if (idx == -1)
            throw new NamingException("missing '=', parameter = " + par);
        return par.substring(0, idx).trim();
    }

    private static String getValue(String par) throws NamingException {
        int idx = par.indexOf('=');
        if (idx == -1)
            throw new NamingException("missing '=', parameter = " + par);
        return par.substring(idx + 1).trim();
    }

    public static JNDIInfo parseURL(String ref)
            throws NamingException {
        JNDIInfo jndiInfo = null;
        if (!ref.startsWith("smqp://"))
            throw new NamingException("invalid URL: protocol != smqp, URL=" + ref);
        String s = ref.substring(7);
        String username = null;
        String password = null;
        if (s.indexOf('@') != -1) {
            // at least username
            String up = s.substring(0, s.indexOf('@'));
            if (up.indexOf(':') != -1) {
                // and a passwd
                username = up.substring(0, up.indexOf(':'));
                password = up.substring(username.length() + 1);
            } else {
                // only username
                username = up;
            }
            s = s.substring(s.indexOf('@') + 1);
        }
        String factory = DEFAULT_FACTORY;
        long timeout = DEFAULT_TIMEOUT;
        long keepalive = DEFAULT_KEEPALIVE;
        String hostname = null;
        String sport = null;
        int port = 0;
        boolean reconnect = false;
        long retrydelay = 0;
        int maxretries = 0;
        String host2 = null;
        int port2 = 0;
        boolean debug = false;
        boolean hasParameters = false;
        if (s.indexOf(':') != -1) {
            hostname = s.substring(0, s.indexOf(':'));
            s = s.substring(s.indexOf(':') + 1);
            if (s.indexOf('/') == -1)
                sport = s;
            else
                sport = s.substring(0, s.indexOf('/'));
            try {
                port = Integer.parseInt(sport);
            } catch (NumberFormatException nfe) {
                throw new NamingException("invalid port number (" + sport + "), URL=" + ref);
            }
        } else {
            if (s.indexOf('/') == -1)
                hostname = s;
            else
                hostname = s.substring(0, s.indexOf('/'));
            if (!hostname.toLowerCase().equals(INTRAVM))
                throw new NamingException("Expected '" + INTRAVM + "' but got '" + hostname + "'");
        }
        if (s.indexOf('/') != -1) {
            String[] pars = getPars(s.substring(s.indexOf('/') + 1));
            hasParameters = pars.length > 0;
            for (int i = 0; i < pars.length; i++) {
                String name = getName(pars[i]);
                String value = getValue(pars[i]);
                if (name.equals("type")) {
                    factory = value;
                } else if (name.equals("timeout")) {
                    try {
                        timeout = Long.parseLong(value);
                        if (timeout < 0)
                            throw new NamingException("timeout < 0: " + pars[i]);
                    } catch (NumberFormatException nfe) {
                        throw new NamingException("invalid long value: " + pars[i]);
                    }
                } else if (name.equals("keepalive")) {
                    try {
                        keepalive = Long.parseLong(value);
                        if (keepalive < 0)
                            throw new NamingException("keepalive < 0: " + pars[i]);
                    } catch (NumberFormatException nfe) {
                        throw new NamingException("invalid long value: " + pars[i]);
                    }
                } else if (name.equals("reconnect")) {
                    reconnect = Boolean.valueOf(value).booleanValue();
                } else if (name.equals("retrydelay")) {
                    try {
                        retrydelay = Long.parseLong(value);
                        if (retrydelay < 0)
                            throw new NamingException("retrydelay < 0: " + pars[i]);
                    } catch (NumberFormatException nfe) {
                        throw new NamingException("invalid long value: " + pars[i]);
                    }
                } else if (name.equals("maxretries")) {
                    try {
                        maxretries = Integer.parseInt(value);
                        if (maxretries < 0)
                            throw new NamingException("maxretries < 0: " + pars[i]);
                    } catch (NumberFormatException nfe) {
                        throw new NamingException("invalid long value: " + pars[i]);
                    }
                } else if (name.equals("host2")) {
                    host2 = value == null ? hostname : value;
                } else if (name.equals("port2")) {
                    try {
                        port2 = Integer.parseInt(value);
                        if (port2 < 0)
                            throw new NamingException("port2 < 0: " + pars[i]);
                    } catch (NumberFormatException nfe) {
                        throw new NamingException("invalid long value: " + pars[i]);
                    }
                } else if (name.equals("debug")) {
                    debug = Boolean.valueOf(value).booleanValue();
                } else
                    throw new NamingException("invalid parameter: " + pars[i]);
            }
        }
        jndiInfo = new JNDIInfo(username, password, hostname, port, factory, timeout, keepalive, hostname.equals(INTRAVM), reconnect, retrydelay, maxretries, host2, port2, debug, hasParameters);
        return jndiInfo;
    }
}

