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

package com.swiftmq.jms.v750;

import com.swiftmq.util.Version;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Enumeration;
import java.util.NoSuchElementException;

public class ConnectionMetaDataImpl implements ConnectionMetaData, Enumeration {
    static final String[] jmsxEnum = {"JMSXDeliveryCount", "JMSXGroupID", "JMSXGroupSeq", "JMSXUserID"};
    int enumPos = 0;
    String jmsVersion = null;
    int jmsMajorVersion = 0;
    int jmsMinorVersion = 0;
    String jmsProviderName = null;
    String jmsProviderVersion = null;
    int jmsProviderMajorVersion = 0;
    int jmsProviderMinorVersion = 0;
    String routerName = null;

    public ConnectionMetaDataImpl(String jmsVersion, int jmsMajorVersion,
                                  int jmsMinorVersion, String jmsProviderName,
                                  String jmsProviderVersion,
                                  int jmsProviderMajorVersion,
                                  int jmsProviderMinorVersion, String routerName) {
        this.jmsVersion = jmsVersion;
        this.jmsMajorVersion = jmsMajorVersion;
        this.jmsMinorVersion = jmsMinorVersion;
        this.jmsProviderName = jmsProviderName;
        this.jmsProviderVersion = jmsProviderVersion;
        this.jmsProviderMajorVersion = jmsProviderMajorVersion;
        this.jmsProviderMinorVersion = jmsProviderMinorVersion;
        this.routerName = routerName;
    }

    public ConnectionMetaDataImpl(String routerName) {
        this("1.1", 1, 1, "SwiftMQ", Version.getKernelVersion(), Version.getSwiftmqMajorVersion(), Version.getSwiftmqMinorVersion(), routerName);
    }

    public ConnectionMetaDataImpl() {
    }

    public void writeContent(DataOutput out) throws IOException {
        if (jmsVersion == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            out.writeUTF(jmsVersion);
        }

        out.writeInt(jmsMajorVersion);
        out.writeInt(jmsMinorVersion);

        if (jmsProviderName == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            out.writeUTF(jmsProviderName);
        }

        if (jmsProviderVersion == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            out.writeUTF(jmsProviderVersion);
        }

        out.writeInt(jmsProviderMajorVersion);
        out.writeInt(jmsProviderMinorVersion);

        if (routerName == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            out.writeUTF(routerName);
        }
    }

    public void readContent(DataInput in) throws IOException {
        byte set = in.readByte();

        if (set == 0) {
            jmsVersion = null;
        } else {
            jmsVersion = in.readUTF();
        }

        jmsMajorVersion = in.readInt();
        jmsMinorVersion = in.readInt();
        set = in.readByte();

        if (set == 0) {
            jmsProviderName = null;
        } else {
            jmsProviderName = in.readUTF();
        }

        set = in.readByte();

        if (set == 0) {
            jmsProviderVersion = null;
        } else {
            jmsProviderVersion = in.readUTF();
        }

        jmsProviderMajorVersion = in.readInt();
        jmsProviderMinorVersion = in.readInt();

        set = in.readByte();

        if (set == 0) {
            routerName = null;
        } else {
            routerName = in.readUTF();
        }
    }

    public String getJMSVersion() throws JMSException {
        return (jmsVersion);
    }

    public int getJMSMajorVersion() throws JMSException {
        return (jmsMajorVersion);
    }

    public int getJMSMinorVersion() throws JMSException {
        return (jmsMinorVersion);
    }

    public String getJMSProviderName() throws JMSException {
        return (jmsProviderName);
    }

    public String getProviderVersion() throws JMSException {
        return (jmsProviderVersion);
    }

    public int getProviderMajorVersion() throws JMSException {
        return (jmsProviderMajorVersion);
    }

    public int getProviderMinorVersion() throws JMSException {
        return (jmsProviderMinorVersion);
    }

    public String getRouterName() {
        return routerName;
    }

    public Enumeration getJMSXPropertyNames() throws JMSException {
        enumPos = 0;
        return this;
    }

    public boolean hasMoreElements() {
        return enumPos < jmsxEnum.length;
    }

    public Object nextElement() throws NoSuchElementException {
        if (enumPos >= jmsxEnum.length)
            throw new NoSuchElementException("no more elements found");
        return jmsxEnum[enumPos++];
    }

    public String toString() {
        return "[ConnectionMetaDataImpl " + "JMSVersion=" + jmsVersion + " "
                + "JMSMajorVersion=" + jmsMajorVersion + " " + "JMSMinorVersion="
                + jmsMinorVersion + " " + "JMSProviderName=" + jmsProviderName
                + " " + "JMSProviderVersion=" + jmsProviderVersion + " "
                + "JMSProviderMajorVersion=" + jmsProviderMajorVersion + " "
                + "JMSProviderMinorVersion=" + jmsProviderMinorVersion + " routerName=" + routerName + "]";
    }
}



