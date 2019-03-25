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

package com.swiftmq.jndi.fs;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.Dom4JDriver;

import javax.naming.*;
import java.io.*;
import java.util.Hashtable;

public class ContextImpl implements Context, java.io.Serializable, NameParser {
    File contextDir = null;
    XStream xStream = null;

    ContextImpl(File contextDir) {
        this.contextDir = contextDir;
        xStream = new XStream(new Dom4JDriver());
    }

    public Name parse(String s) throws NamingException {
        return new CompositeName(s);
    }

    public Object lookup(Name name) throws NamingException {
        return lookup(name.get(0));
    }

    public Object lookup(String s) throws NamingException {
        if (s == null || s.equals("") || s.equals("/"))
            return this;
        Object o = null;
        File file = new File(contextDir, s + ".xml");
        if (file.exists()) {
            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                o = xStream.fromXML(bufferedReader);
            } catch (FileNotFoundException e) {
            }
        }
        return o;
    }

    public void bind(Name name, Object o) throws NamingException {
        bind(name.get(0), o);
    }

    public void bind(String s, Object o) throws NamingException {
        try {
            File file = new File(contextDir, s + ".xml");
            if (file.exists())
                throw new NameAlreadyBoundException("Name already bound: " + s);
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
            xStream.toXML(o, bufferedWriter);
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            throw new NamingException(e.toString());
        }
    }

    public void rebind(Name name, Object o) throws NamingException {
        rebind(name.get(0), o);
    }

    public void rebind(String s, Object o) throws NamingException {
        unbind(s);
        bind(s, o);
    }

    public void unbind(Name name) throws NamingException {
        unbind(name.get(0));
    }

    public void unbind(String s) throws NamingException {
        File file = new File(contextDir, s + ".xml");
        if (file.exists())
            file.delete();
    }

    public void rename(Name name, Name name1) throws NamingException {
        rename(name.get(0), name1.get(0));
    }

    public void rename(String s, String s1) throws NamingException {
        File file = new File(contextDir, s + ".xml");
        if (file.exists())
            file.renameTo(new File(s1 + ".xml"));
        else
            throw new NameNotFoundException("Name not found: " + s);
    }

    public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
        return null;
    }

    public NamingEnumeration<NameClassPair> list(String s) throws NamingException {
        return null;
    }

    public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
        return null;
    }

    public NamingEnumeration<Binding> listBindings(String s) throws NamingException {
        return null;
    }

    public void destroySubcontext(Name name) throws NamingException {
    }

    public void destroySubcontext(String s) throws NamingException {
    }

    public Context createSubcontext(Name name) throws NamingException {
        return this;
    }

    public Context createSubcontext(String s) throws NamingException {
        return this;
    }

    public Object lookupLink(Name name) throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public Object lookupLink(String s) throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public NameParser getNameParser(Name name) throws NamingException {
        return this;
    }

    public NameParser getNameParser(String s) throws NamingException {
        return this;
    }

    public Name composeName(Name name, Name name1) throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public String composeName(String s, String s1) throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public Object addToEnvironment(String s, Object o) throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public Object removeFromEnvironment(String s) throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public Hashtable<?, ?> getEnvironment() throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }

    public void close() throws NamingException {
    }

    public String getNameInNamespace() throws NamingException {
        throw new OperationNotSupportedException("not supported");
    }
}
