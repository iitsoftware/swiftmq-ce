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

package com.swiftmq.jms;

import com.swiftmq.tools.security.SecureClassLoaderObjectInputStream;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;
import java.io.*;

/**
 * Implementation of a ObjectMessage. There is one additional method <code>getObject(classLoader)</code>
 * which must be used by Extension Swiftlet, passing their own classloader to construct the object.
 * This is required due to hot deployment of Extension Swiftlets and different class loaders.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class ObjectMessageImpl extends MessageImpl implements ObjectMessage {
    static final String PROP_BUFFERSIZE = "swiftmq.jms.objectmessage.buffersize";
    static final int BUFFERSIZE = Integer.parseInt(System.getProperty(PROP_BUFFERSIZE, "8192"));
    boolean bodyReadOnly = false;
    byte[] array = null;
    int cnt = 0;
    boolean useThreadContextCL = false;

    protected int getType() {
        return TYPE_OBJECTMESSAGE;
    }

    protected void writeBody(DataOutput out) throws IOException {
        if (cnt > 0) {
            out.writeByte(1);
            out.writeInt(cnt);
            out.write(array, 0, cnt);
        } else {
            out.writeByte(0);
        }
    }

    protected void readBody(DataInput in) throws IOException {
        byte set = in.readByte();
        if (set == 1) {
            array = new byte[in.readInt()];
            in.readFully(array);
            cnt = array.length;
        } else
            cnt = 0;
    }

    public void setUseThreadContextCL(boolean useThreadContextCL) {
        this.useThreadContextCL = useThreadContextCL;
    }

    public boolean isUseThreadContextCL() {
        return useThreadContextCL;
    }

    private void serialize(Serializable obj) throws IOException {
        DataByteArrayOutputStream bos = new DataByteArrayOutputStream(BUFFERSIZE);
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.close();
        array = bos.getBuffer();
        cnt = bos.getCount();
    }

    private Serializable deserialize(byte[] b, ClassLoader loader) throws IOException, ClassNotFoundException {
        DataByteArrayInputStream bis = new DataByteArrayInputStream(b);
        ObjectInputStream ois = new SecureClassLoaderObjectInputStream(loader == null ? bis : bis, loader);
        Serializable obj = (Serializable) ois.readObject();
        ois.close();
        return obj;
    }

    /**
     * Set the serializable object containing this message's data.
     *
     * @param object the message's data
     * @throws JMSException                 if JMS fails to  set object due to
     *                                      some internal JMS error.
     * @throws MessageFormatException       if object serialization fails
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setObject(Serializable obj) throws JMSException {
        if (bodyReadOnly) {
            throw new MessageNotWriteableException("Message is read only");
        }
        if (obj == null)
            cnt = 0;
        else {
            try {
                serialize(obj);
            } catch (Exception e) {
                throw new JMSException(e.toString());
            }
        }
    }

    /**
     * Get the serializable object containing this message's data. The
     * default value is null.
     *
     * @param customLoader a custom class loader to load the class
     * @return the serializable object containing this message's data
     * @throws JMSException           if JMS fails to  get object due to
     *                                some internal JMS error.
     * @throws MessageFormatException if object deserialization fails
     */
    public Serializable getObject(ClassLoader customLoader) throws JMSException {
        if (cnt > 0) {
            try {
                return deserialize(array, customLoader);
            } catch (Exception e) {
                throw new JMSException(e.toString());
            }
        }
        return null;
    }

    public void setReadOnly(boolean b) {
        super.setReadOnly(b);
        bodyReadOnly = b;
    }

    /**
     * Get the serializable object containing this message's data. The
     * default value is null.
     *
     * @return the serializable object containing this message's data
     * @throws JMSException           if JMS fails to  get object due to
     *                                some internal JMS error.
     * @throws MessageFormatException if object deserialization fails
     */
    public Serializable getObject() throws JMSException {
        return getObject(useThreadContextCL ? Thread.currentThread().getContextClassLoader() : null);
    }

    /**
     * Clear out the message body. All other parts of the message are left
     * untouched.
     *
     * @throws JMSException if JMS fails to due to some internal JMS error.
     */
    public void clearBody() throws JMSException {
        super.clearBody();
        bodyReadOnly = false;
        cnt = 0;
    }

    public String toString() {
        StringBuffer b = new StringBuffer("[ObjectMessageImpl ");
        b.append(super.toString());
        b.append(" array=");
        b.append(array);
        b.append(" cnt=");
        b.append(cnt);
        b.append("]");
        return b.toString();
    }

    private static class CustomCLObjectInputStream extends ObjectInputStream {
        ClassLoader customLoader;

        CustomCLObjectInputStream(InputStream in, ClassLoader customLoader) throws IOException, StreamCorruptedException {
            super(in);
            this.customLoader = customLoader;
        }

        protected Class resolveClass(ObjectStreamClass v)
                throws IOException, ClassNotFoundException {
            return Class.forName(v.getName(), false, customLoader);
        }
    }
}



