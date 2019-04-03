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

/*--- formatted by Jindent 2.1, (www.c-lab.de/~jindent) ---*/

package com.swiftmq.jms.v400;

import com.swiftmq.util.Version;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Enumeration;
import java.util.NoSuchElementException;

/**
 * @Author Andreas Mueller, IIT GmbH
 * @Version 1.0
 */
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

    /**
     * @SBGen Constructor assigns jmsVersion, jmsMajorVersion, jmsMinorVersion, jmsProviderName, jmsProviderVersion, jmsProviderMajorVersion, jmsProviderMinorVersion
     */
    public ConnectionMetaDataImpl(String jmsVersion, int jmsMajorVersion,
                                  int jmsMinorVersion, String jmsProviderName,
                                  String jmsProviderVersion,
                                  int jmsProviderMajorVersion,
                                  int jmsProviderMinorVersion) {

        // SBgen:  Assign variables
        this.jmsVersion = jmsVersion;
        this.jmsMajorVersion = jmsMajorVersion;
        this.jmsMinorVersion = jmsMinorVersion;
        this.jmsProviderName = jmsProviderName;
        this.jmsProviderVersion = jmsProviderVersion;
        this.jmsProviderMajorVersion = jmsProviderMajorVersion;
        this.jmsProviderMinorVersion = jmsProviderMinorVersion;

        // SBgen:  End assign
    }

    /**
     * Constructor declaration
     *
     * @see
     */
    public ConnectionMetaDataImpl() {
        this("1.1", 1, 1, "SwiftMQ", Version.getKernelVersion(), Version.getSwiftmqMajorVersion(), Version.getSwiftmqMinorVersion());
    }

    /**
     * Write the content of this object to the stream.
     *
     * @param out output stream
     * @throws IOException if an error occurs
     */
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
    }

    /**
     * Read the content of this object from the stream.
     *
     * @param in input stream
     * @throws IOException if an error occurs
     */
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
    }

    /**
     * Get the JMS version.
     *
     * @return the JMS version.
     */
    public String getJMSVersion() throws JMSException {
        return (jmsVersion);
    }

    /**
     * Get the JMS major version number.
     *
     * @return the JMS major version number.
     */
    public int getJMSMajorVersion() throws JMSException {
        return (jmsMajorVersion);
    }

    /**
     * Get the JMS minor version number.
     *
     * @return the JMS minor version number.
     */
    public int getJMSMinorVersion() throws JMSException {
        return (jmsMinorVersion);
    }

    /**
     * Get the JMS provider name.
     *
     * @return the JMS provider name.
     */
    public String getJMSProviderName() throws JMSException {
        return (jmsProviderName);
    }

    /**
     * Get the JMS provider version.
     *
     * @return the JMS provider version.
     */
    public String getProviderVersion() throws JMSException {
        return (jmsProviderVersion);
    }

    /**
     * Get the JMS provider major version number.
     *
     * @return the JMS provider major version number.
     */
    public int getProviderMajorVersion() throws JMSException {
        return (jmsProviderMajorVersion);
    }

    /**
     * Get the JMS provider minor version number.
     *
     * @return the JMS provider minor version number.
     */
    public int getProviderMinorVersion() throws JMSException {
        return (jmsProviderMinorVersion);
    }

    /**
     * Get an enumeration of JMSX Property Names.
     *
     * @return an Enumeration of JMSX PropertyNames.
     * @throws JMSException if some internal error occurs in
     *                      JMS implementation during the property
     *                      names retrieval.
     */

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

    /**
     * Method declaration
     *
     * @return
     * @see
     */
    public String toString() {
        return "[ConnectionMetaDataImpl " + "JMSVersion=" + jmsVersion + " "
                + "JMSMajorVersion=" + jmsMajorVersion + " " + "JMSMinorVersion="
                + jmsMinorVersion + " " + "JMSProviderName=" + jmsProviderName
                + " " + "JMSProviderVersion=" + jmsProviderVersion + " "
                + "JMSProviderMajorVersion=" + jmsProviderMajorVersion + " "
                + "JMSProviderMinorVersion=" + jmsProviderMinorVersion + "]";
    }

}



