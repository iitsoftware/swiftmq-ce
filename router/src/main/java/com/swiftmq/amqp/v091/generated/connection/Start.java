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

package com.swiftmq.amqp.v091.generated.connection;

/**
 * AMQP-Protocol Version 091
 * Automatically generated, don't change!
 * Generation Date: Thu Apr 12 12:18:24 CEST 2012
 * (c) 2012, IIT Software GmbH, Bremen/Germany
 * All Rights Reserved
 **/

import com.swiftmq.amqp.v091.io.BitSupportDataInput;
import com.swiftmq.amqp.v091.io.BitSupportDataOutput;
import com.swiftmq.amqp.v091.types.Coder;

import java.io.IOException;
import java.util.Map;

public class Start extends ConnectionMethod {
    byte versionMajor;
    byte versionMinor;
    Map<String, Object> serverProperties;
    byte[] mechanisms;
    byte[] locales;

    public Start() {
        _classId = 10;
        _methodId = 10;
    }

    public void accept(ConnectionMethodVisitor visitor) {
        visitor.visit(this);
    }

    public byte getVersionMajor() {
        return versionMajor;
    }

    public void setVersionMajor(byte versionMajor) {
        this.versionMajor = versionMajor;
    }

    public byte getVersionMinor() {
        return versionMinor;
    }

    public void setVersionMinor(byte versionMinor) {
        this.versionMinor = versionMinor;
    }

    public Map<String, Object> getServerProperties() {
        return serverProperties;
    }

    public void setServerProperties(Map<String, Object> serverProperties) {
        this.serverProperties = serverProperties;
    }

    public byte[] getMechanisms() {
        return mechanisms;
    }

    public void setMechanisms(byte[] mechanisms) {
        this.mechanisms = mechanisms;
    }

    public byte[] getLocales() {
        return locales;
    }

    public void setLocales(byte[] locales) {
        this.locales = locales;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        versionMajor = Coder.readByte(in);
        versionMinor = Coder.readByte(in);
        serverProperties = Coder.readTable(in);
        mechanisms = Coder.readLongString(in);
        locales = Coder.readLongString(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeByte(versionMajor, out);
        Coder.writeByte(versionMinor, out);
        Coder.writeTable(serverProperties, out);
        Coder.writeLongString(mechanisms, out);
        Coder.writeLongString(locales, out);
        out.bitFlush();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer(" ");
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("versionMajor=");
        b.append(versionMajor);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("versionMinor=");
        b.append(versionMinor);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("serverProperties=");
        b.append(serverProperties);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("mechanisms=");
        if (mechanisms != null)
            b.append(new String(mechanisms));
        else
            b.append("null");
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("locales=");
        if (locales != null)
            b.append(new String(locales));
        else
            b.append("null");
        return b.toString();
    }

    public String toString() {
        return "[Start " + super.toString() + getDisplayString() + "]";
    }
}
