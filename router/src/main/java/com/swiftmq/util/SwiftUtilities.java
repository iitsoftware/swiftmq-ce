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

package com.swiftmq.util;

import com.swiftmq.swiftlet.SwiftletManager;

import javax.jms.Session;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class SwiftUtilities {
    public static final String PREFIX = "${";
    public static final String SUFFIX = "}";
    public static final String ABSOLUTEDIR_PREFIX = "absolute:";
    public static final String UPGRADE_ATTRIBUTE = "_upgrade";

    public static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    public static String extractAMQPName(String url) throws MalformedURLException {
        String s = url.replaceFirst("amqp:", "http:").replaceFirst("amqps:", "https:");
        if (!s.startsWith("http://") && !s.startsWith("https://"))
            s = "http://" + s;
        String result = new URL(s).getFile().replaceFirst("/", "");
        if (result == null || result.equals(""))
            return url;
        if (result.indexOf('?') != -1)
            return result.substring(0, result.indexOf('?'));
        return result;
    }

    public static String substitute(String source, String varName, String varValue) {
        String result = new String(source);
        String var = PREFIX + varName + SUFFIX;
        int idx = result.indexOf(var);
        while (idx != -1) {
            StringBuffer s = new StringBuffer(result.substring(0, idx));
            s.append(varValue);
            s.append(result.substring(idx + var.length()));
            result = s.toString();
            idx = result.indexOf(var);
        }
        return result;
    }

    public static String replace(String source, String from, String to) {
        String result = new String(source);
        String var = from;
        int idx = result.indexOf(var);
        if (idx != -1) {
            StringBuffer s = new StringBuffer(result.substring(0, idx));
            s.append(to);
            s.append(result.substring(idx + var.length()));
            result = s.toString();
        }
        return result;
    }

    public static String substitute(String source, String[] varValue) {
        String result = new String(source);
        String var = PREFIX + "*" + SUFFIX;
        int idx = result.indexOf(var);
        while (idx != -1) {
            StringBuffer s = new StringBuffer(result.substring(0, idx));
            s.append(concat(varValue, " "));
            s.append(result.substring(idx + var.length()));
            result = s.toString();
            idx = result.indexOf(var);
        }
        return result;
    }

    public static String addWorkingDir(String filename) {
        String fn = filename;
        boolean absolute = Boolean.valueOf(System.getProperty("swiftmq.paths.absolute")).booleanValue();
        boolean absInFilename = filename.startsWith(ABSOLUTEDIR_PREFIX);
        if (absInFilename)
            fn = fn.substring(ABSOLUTEDIR_PREFIX.length());
        if (absolute || absInFilename)
            return fn;
        fn = SwiftletManager.getInstance().getWorkingDirectory() + File.separatorChar + fn;
        return fn;
    }

    public static void createDirectoryOfFile(String filename) throws Exception {
        boolean autocreate = Boolean.valueOf(System.getProperty("swiftmq.directory.autocreate")).booleanValue();
        if (autocreate) {
            String path = new File(filename).getParent();
            if (path != null) {
                File d = new File(path);
                if (!d.exists())
                    d.mkdir();
            }
        }
    }

    // Name checker methods Character

    public static void verifyClientId(String s) throws Exception {
        if (s == null)
            throw new NullPointerException("name is null");
        for (int i = 0; i < s.length(); i++) {
            if (!(Character.isLetterOrDigit(s.charAt(i)) ||
                    s.charAt(i) == '_' ||
                    s.charAt(i) == '-'))
                throw new Exception(s + ": invalid character found '" + s.charAt(i) + "';must be letter, digit, or '-', '_'");
        }
    }

    public static void verifyDurableName(String s) throws Exception {
        verifyClientId(s);
    }

    public static void verifyRouterName(String s) throws Exception {
        if (s == null)
            throw new NullPointerException("name is null");
        for (int i = 0; i < s.length(); i++) {
            if (!(Character.isLetterOrDigit(s.charAt(i)) ||
                    s.charAt(i) == '.' || s.charAt(i) == '_' ||
                    s.charAt(i) == '-'))
                throw new Exception(s + ": invalid character found '" + s.charAt(i) + "'; must be letter, digit, or one of '._-'");
        }
    }

    public static void verifyTopicName(String s) throws Exception {
        if (s == null)
            throw new NullPointerException("name is null");
        for (int i = 0; i < s.length(); i++) {
            if (!(Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '.' || s.charAt(i) == '-' || s.charAt(i) == '_'))
                throw new Exception(s + ": invalid character found '" + s.charAt(i) + "'; must be letter, digit, or '.', '-', '_'");
        }
    }

    public static void verifyQueueName(String s) throws Exception {
        if (s == null)
            throw new NullPointerException("name is null");
        if (s.indexOf('@') == -1)
            verifyLocalQueueName(s);
        else {
            String[] a = tokenize(s, "@");
            verifyLocalQueueName(a[0]);
            if (a.length > 1)
                verifyRouterName(a[1]);
        }
    }

    public static void verifyLocalQueueName(String s) throws Exception {
        verifyClientId(s);
    }

    public static void verifyUserName(String s) throws Exception {
        verifyClientId(s);
    }

    public static String[] tokenize(String s, String delimiter) {
        StringTokenizer t = new StringTokenizer(s, delimiter);
        String[] r = new String[t.countTokens()];
        int i = 0;
        while (t.hasMoreTokens())
            r[i++] = t.nextToken();
        return r;
    }

    public static String[] parseCLICommand(String s) throws Exception {
        ArrayList al = new ArrayList();
        boolean openQuote = false;
        boolean doubleQuote = false;
        StringBuffer token = new StringBuffer();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case ' ':
                    if (openQuote)
                        token.append(c);
                    else {
                        if (token.toString().trim().length() > 0)
                            al.add(token.toString());
                        token = new StringBuffer();
                    }
                    break;
                case '"':
                    if (openQuote) {
                        if (doubleQuote) {
                            token.append(c);
                            doubleQuote = false;
                        } else if (i < s.length() - 1 && s.charAt(i + 1) == '"')
                            doubleQuote = true;
                        else {
                            openQuote = false;
                            doubleQuote = false;
                        }
                    } else {
                        openQuote = true;
                        doubleQuote = false;
                    }
                    break;
                default:
                    token.append(c);
                    break;
            }
        }
        if (openQuote)
            throw new Exception("Syntax error, missing \"");
        if (token.toString().trim().length() > 0)
            al.add(token.toString());
        return (String[]) al.toArray(new String[al.size()]);
    }

    public static List parseCLICommandList(String s) throws Exception {
        ArrayList cmdList = new ArrayList();
        ArrayList tokenList = new ArrayList();
        boolean openQuote = false;
        boolean doubleQuote = false;
        StringBuffer token = new StringBuffer();
        boolean isAliasSet = s.startsWith("aset ");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case ' ':
                    if (openQuote)
                        token.append(c);
                    else {
                        tokenList.add(token.toString());
                        token = new StringBuffer();
                    }
                    break;
                case ';':
                    if (openQuote || isAliasSet)
                        token.append(c);
                    else {
                        tokenList.add(token.toString());
                        cmdList.add(tokenList.toArray(new String[tokenList.size()]));
                        token = new StringBuffer();
                        tokenList.clear();
                    }
                    break;
                case '"':
                    if (openQuote) {
                        if (doubleQuote) {
                            token.append(c);
                            doubleQuote = false;
                        } else if (i < s.length() - 1 && s.charAt(i + 1) == '"')
                            doubleQuote = true;
                        else {
                            openQuote = false;
                            doubleQuote = false;
                        }
                    } else {
                        openQuote = true;
                        doubleQuote = false;
                    }
                    break;
                default:
                    token.append(c);
                    break;
            }
        }
        if (openQuote)
            throw new Exception("Syntax error, missing \"");
        if (token.length() > 0) {
            tokenList.add(token.toString());
            cmdList.add(tokenList.toArray(new String[tokenList.size()]));
        }
        return cmdList;
    }

    public static String concat(String[] s, String delimiter) {
        StringBuffer b = new StringBuffer();
        for (int i = 0; i < s.length; i++) {
            if (i > 0)
                b.append(delimiter);
            b.append(s[i]);
        }
        return b.toString();
    }

    public static String fillToLength(String s, int length) {
        StringBuffer b = new StringBuffer();
        b.append(s);
        for (int i = s.length(); i < length; i++)
            b.append(' ');
        return b.toString();
    }

    public static String fillLeft(String s, int length, char filler) {
        StringBuffer b = new StringBuffer();
        for (int i = 0; i < length - s.length(); i++)
            b.append(filler);
        b.append(s);
        return b.toString();
    }

    public static String[] cutLast(String[] s) {
        if (s == null || s.length == 1)
            return null;
        String[] cutted = new String[s.length - 1];
        for (int i = 0; i < cutted.length; i++)
            cutted[i] = s[i];
        return cutted;
    }

    public static String[] cutFirst(String[] s) {
        if (s == null || s.length == 1)
            return null;
        String[] cutted = new String[s.length - 1];
        for (int i = 1; i < s.length; i++)
            cutted[i - 1] = s[i];
        return cutted;
    }

    public static String[] concat(String r, String[] s) {
        String[] sb = new String[s.length + 1];
        sb[0] = r;
        System.arraycopy(s, 0, sb, 1, s.length);
        return sb;
    }

    public static String[] append(String[] s, String[] a) {
        if (s == null)
            return a;
        String[] appended = new String[s.length + a.length];
        for (int i = 0; i < s.length; i++)
            appended[i] = s[i];
        for (int i = 0; i < a.length; i++)
            appended[i + s.length] = a[i];
        return appended;
    }

    public static void showActiveThreads(OutputStream out) {
        PrintWriter writer = new PrintWriter(out, true);
        ThreadGroup parent;
        ThreadGroup rootGroup;

        parent = Thread.currentThread().getThreadGroup();
        do {
            rootGroup = parent;
            parent = parent.getParent();
        } while (parent != null);

        writer.println("ThreadGroups: "
                + (rootGroup.activeGroupCount() + 1));

        writer.println(rootGroup.toString());
        ThreadGroup threadGroups[]
                = new ThreadGroup[rootGroup.activeGroupCount()];
        rootGroup.enumerate(threadGroups);

        for (int i = 0; i < threadGroups.length; i++) {
            writer.println(threadGroups[i].toString());
            writer.println("\tThreads: " + threadGroups[i].activeCount());

            Thread threads[] = new Thread[threadGroups[i].activeCount()];
            threadGroups[i].enumerate(threads);

            for (int j = 0; j < threads.length; j++) {
                writer.println("\t\t" + threads[j]);
            }
        }
    }

    public static String ackModeToString(int ackMode) {
        String s = null;
        switch (ackMode) {
            case Session.AUTO_ACKNOWLEDGE:
                s = "AUTO_ACKNOWLEDGE";
                break;
            case Session.CLIENT_ACKNOWLEDGE:
                s = "CLIENT_ACKNOWLEDGE";
                break;
            case Session.DUPS_OK_ACKNOWLEDGE:
                s = "DUPS_OK_ACKNOWLEDGE";
                break;
        }
        return s;
    }

    public static int persistenceModeToInt(String pm) {
        if (pm.equals("as_message"))
            return 0;
        if (pm.equals("persistent"))
            return 1;
        return 2;
    }

    public static Object getFirstStartsWith(Map map, String startsWithKey) {
        Object obj = null;
        for (Iterator iter = map.keySet().iterator(); iter.hasNext(); ) {
            String k = (String) iter.next();
            if (k.startsWith(startsWithKey)) {
                obj = map.get(k);
                break;
            }
        }
        return obj;
    }

    public static byte[] loadImageAsBytes(InputStream instream) {
        byte[] array = null;
        try {
            BufferedInputStream in = new BufferedInputStream(instream);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int c = 0;
            while ((c = in.read()) != -1) {
                out.write((byte) c);
            }
            array = out.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return array;
    }

}

