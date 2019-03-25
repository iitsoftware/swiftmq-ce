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

import com.swiftmq.jms.SwiftMQConnectionFactory;

import javax.naming.*;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;

public class ReconnectContext implements Context {
    // A list of Hashtables
    List envList = null;
    int maxRetries = 0;
    long retryDelay = 0;
    Context current = null;
    int currentPos = -1;
    boolean closed = false;
    boolean debug = false;
    String connectURL = null;

    public ReconnectContext(List envList, int maxRetries, long retryDelay) {
        this.envList = envList;
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
        // To ensure executing static initializer
        new SwiftMQConnectionFactory();
        if (debug)
            System.out.println(new Date() + " " + toString() + "/created: envList=" + envList + ", maxRetries=" + maxRetries + ", retryDelay=" + retryDelay);
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    private void reconnect() throws CommunicationException {
        if (debug) System.out.println(new Date() + " " + toString() + "/reconnect, current=" + current);
        int retries = maxRetries;
        do {
            if (debug)
                System.out.println(new Date() + " " + toString() + "/reconnect, current=" + current + ", retries=" + retries);
            if (retries < maxRetries && retryDelay > 0) {
                try {
                    if (debug)
                        System.out.println(new Date() + " " + toString() + "/reconnect, current=" + current + ", waiting " + retryDelay);
                    synchronized (this) {
                        wait(retryDelay);
                    }
                } catch (InterruptedException e) {
                }
                if (debug)
                    System.out.println(new Date() + " " + toString() + "/reconnect, current=" + current + ", continue...");
                if (closed)
                    return;
            }
            if (current != null) {
                try {
                    if (debug)
                        System.out.println(new Date() + " " + toString() + "/reconnect, current=" + current + ", closing old one");
                    current.close();
                } catch (Exception e) {
                }
                current = null;
            }
            if (currentPos == envList.size() - 1)
                currentPos = -1;
            currentPos++;
            try {
                if (debug)
                    System.out.println(new Date() + " " + toString() + "/reconnect, current=" + current + ", trying: " + envList.get(currentPos));
                current = new InitialContextFactoryImpl().getInitialContext((Hashtable) envList.get(currentPos));
                if (current != null)
                    connectURL = (String) (((Hashtable) envList.get(currentPos))).get(Context.PROVIDER_URL);
                if (debug) System.out.println(new Date() + " " + toString() + "/reconnect, connectURL=" + connectURL);
            } catch (Exception e) {
                if (e instanceof StopRetryException)
                    throw new CommunicationException(e.getMessage());
            }
            retries--;
        } while (retries > 0 && current == null);
        if (current == null)
            throw new CommunicationException("Unable to connect, maximum retries (" + maxRetries + ") reached, giving up!");
        if (debug) System.out.println(new Date() + " " + toString() + "/reconnect done, current=" + current);
    }

    private Object runWrapped(Delegation delegation, Object[] parameters) throws NamingException {
        if (debug) System.out.println(new Date() + " " + toString() + "/runWrapped, current=" + current);
        Object obj = null;
        do {
            if (current == null)
                reconnect();
            if (current != null) {
                try {
                    if (debug) System.out.println(new Date() + " " + toString() + "/runWrapped, execute...");
                    obj = delegation.execute(parameters);
                    if (debug)
                        System.out.println(new Date() + " " + toString() + "/runWrapped, execute done, result=" + obj);
                } catch (CommunicationException e) {
                    if (debug)
                        System.out.println(new Date() + " " + toString() + "/runWrapped, CommunicationException=" + e);
                    current = null;
                } catch (NamingException e1) {
                    if (debug) System.out.println(new Date() + " " + toString() + "/runWrapped, NamingException=" + e1);
                }
            }
        } while (!closed && current == null);
        if (debug) System.out.println(new Date() + " " + toString() + "/runWrapped, returning=" + obj);
        return obj;
    }

    public synchronized String getConnectURL() {
        return connectURL;
    }

    public synchronized Object lookup(Name name) throws NamingException {
        return lookup(name.get(0));
    }

    public synchronized Object lookup(String name) throws NamingException {
        return runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.lookup((String) parameter[0]);
            }
        }, new Object[]{name});
    }

    public synchronized void bind(Name name, Object obj) throws NamingException {
        bind(name.get(0), obj);
    }

    public synchronized void bind(String name, Object obj) throws NamingException {
        runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                current.bind((String) parameter[0], parameter[1]);
                return null;
            }
        }, new Object[]{name, obj});
    }

    public synchronized void rebind(Name name, Object obj) throws NamingException {
        rebind(name.get(0), obj);
    }

    public synchronized void rebind(String name, Object obj) throws NamingException {
        runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                current.rebind((String) parameter[0], parameter[1]);
                return null;
            }
        }, new Object[]{name, obj});
    }

    public synchronized void unbind(Name name) throws NamingException {
        unbind(name.get(0));
    }

    public synchronized void unbind(String name) throws NamingException {
        runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                current.unbind((String) parameter[0]);
                return null;
            }
        }, new Object[]{name});
    }

    public synchronized void rename(Name oldName, Name newName) throws NamingException {
        rename(oldName.get(0), newName.get(0));
    }

    public synchronized void rename(String oldName, String newName) throws NamingException {
        runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                current.rename((String) parameter[0], (String) parameter[1]);
                return null;
            }
        }, new Object[]{oldName, newName});
    }

    public synchronized NamingEnumeration list(Name name) throws NamingException {
        return list(name.get(0));
    }

    public synchronized NamingEnumeration list(String name) throws NamingException {
        return (NamingEnumeration) runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.list((String) parameter[0]);
            }
        }, new Object[]{name});
    }

    public synchronized NamingEnumeration listBindings(Name name) throws NamingException {
        return listBindings(name.get(0));
    }

    public synchronized NamingEnumeration listBindings(String name) throws NamingException {
        return (NamingEnumeration) runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.listBindings((String) parameter[0]);
            }
        }, new Object[]{name});
    }

    public synchronized void destroySubcontext(Name name) throws NamingException {
        destroySubcontext(name.get(0));
    }

    public synchronized void destroySubcontext(String name) throws NamingException {
        runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                current.destroySubcontext((String) parameter[0]);
                return null;
            }
        }, new Object[]{name});
    }

    public synchronized Context createSubcontext(Name name) throws NamingException {
        return createSubcontext(name.get(0));
    }

    public synchronized Context createSubcontext(String name) throws NamingException {
        return (Context) runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.createSubcontext((String) parameter[0]);
            }
        }, new Object[]{name});
    }

    public synchronized Object lookupLink(Name name) throws NamingException {
        return lookupLink(name.get(0));
    }

    public synchronized Object lookupLink(String name) throws NamingException {
        return runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.lookupLink((String) parameter[0]);
            }
        }, new Object[]{name});
    }

    public synchronized NameParser getNameParser(Name name) throws NamingException {
        return getNameParser(name.get(0));
    }

    public synchronized NameParser getNameParser(String name) throws NamingException {
        return (NameParser) runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.getNameParser((String) parameter[0]);
            }
        }, new Object[]{name});
    }

    public synchronized Name composeName(Name name, Name prefix) throws NamingException {
        return (Name) runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.composeName((Name) parameter[0], (Name) parameter[1]);
            }
        }, new Object[]{name, prefix});
    }

    public synchronized String composeName(String name, String prefix)
            throws NamingException {
        return (String) runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.composeName((String) parameter[0], (String) parameter[1]);
            }
        }, new Object[]{name, prefix});
    }

    public synchronized Object addToEnvironment(String propName, Object propVal)
            throws NamingException {
        return runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.addToEnvironment((String) parameter[0], (String) parameter[1]);
            }
        }, new Object[]{propName, propVal});
    }

    public synchronized Object removeFromEnvironment(String propName)
            throws NamingException {
        return runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.removeFromEnvironment((String) parameter[0]);
            }
        }, new Object[]{propName});
    }

    public synchronized Hashtable getEnvironment() throws NamingException {
        return (Hashtable) runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.getEnvironment();
            }
        }, null);
    }

    public synchronized String getNameInNamespace() throws NamingException {
        return (String) runWrapped(new Delegation() {
            public Object execute(Object[] parameter) throws NamingException {
                return current.getNameInNamespace();
            }
        }, null);
    }

    public synchronized void close() throws NamingException {
        closed = true;
        if (current != null)
            current.close();
        current = null;
        notify();
    }

    public String toString() {
        return "[ReconnectContext, closed=" + closed + "]";
    }

    private interface Delegation {
        public Object execute(Object[] parameter) throws NamingException;
    }
}
