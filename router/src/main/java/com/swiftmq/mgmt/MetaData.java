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

package com.swiftmq.mgmt;

import com.swiftmq.tools.dump.Dumpable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MetaData implements Dumpable {
    String name;
    String displayName;
    String vendor;
    String version;
    String className;
    String description;

    public MetaData(String displayName, String vendor, String version, String description) {
        // SBgen: Assign variables
        this.displayName = displayName;
        this.vendor = vendor;
        this.version = version;
        this.description = description;
        // SBgen: End assign
    }

    MetaData() {
    }

    public int getDumpId() {
        return MgmtFactory.META;
    }

    private void writeDump(DataOutput out, String s) throws IOException {
        if (s == null)
            out.writeByte(0);
        else {
            out.writeByte(1);
            out.writeUTF(s);
        }
    }

    private String readDump(DataInput in) throws IOException {
        byte set = in.readByte();
        if (set == 1)
            return in.readUTF();
        return null;
    }

    public synchronized void writeContent(DataOutput out)
            throws IOException {
        writeDump(out, displayName);
        writeDump(out, vendor);
        writeDump(out, version);
        writeDump(out, description);
        writeDump(out, className);
    }

    public void readContent(DataInput in)
            throws IOException {
        displayName = readDump(in);
        vendor = readDump(in);
        version = readDump(in);
        description = readDump(in);
        className = readDump(in);
    }

    public void setName(String name) {
        // SBgen: Assign variable
        this.name = name;
    }

    public synchronized void setClassName(String className) {
        // SBgen: Assign variable
        this.className = className;
    }

    /**
     * @return
     * @SBGen Method get name
     */
    public String getName() {
        // SBgen: Get variable
        return (name);
    }

    /**
     * @return
     * @SBGen Method get displayName
     */
    public String getDisplayName() {
        // SBgen: Get variable
        return (displayName);
    }

    /**
     * @return
     * @SBGen Method get vendor
     */
    public String getVendor() {
        // SBgen: Get variable
        return (vendor);
    }

    /**
     * @return
     * @SBGen Method get version
     */
    public String getVersion() {
        // SBgen: Get variable
        return (version);
    }

    /**
     * @return
     * @SBGen Method get className
     */
    public String getClassName() {
        // SBgen: Get variable
        return (className);
    }

    /**
     * @return
     * @SBGen Method get description
     */
    public String getDescription() {
        // SBgen: Get variable
        return (description);
    }

    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("[MetaData, name=");
        s.append(name);
        s.append(", displayName=");
        s.append(displayName);
        s.append(", description=");
        s.append(description);
        s.append(", version=");
        s.append(version);
        s.append(", vendor=");
        s.append(vendor);
        s.append(", className=");
        s.append(className);
        s.append("]");
        return s.toString();
    }
}

