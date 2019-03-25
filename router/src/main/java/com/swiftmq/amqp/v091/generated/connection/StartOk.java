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

public class StartOk extends ConnectionMethod {
    Map<String, Object> clientProperties;
    String mechanism;
    byte[] response;
    String locale;

    public StartOk() {
        _classId = 10;
        _methodId = 11;
    }

    public void accept(ConnectionMethodVisitor visitor) {
        visitor.visit(this);
    }

    public Map<String, Object> getClientProperties() {
        return clientProperties;
    }

    public void setClientProperties(Map<String, Object> clientProperties) {
        this.clientProperties = clientProperties;
    }

    public String getMechanism() {
        return mechanism;
    }

    public void setMechanism(String mechanism) {
        this.mechanism = mechanism;
    }

    public byte[] getResponse() {
        return response;
    }

    public void setResponse(byte[] response) {
        this.response = response;
    }

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    protected void readBody(BitSupportDataInput in) throws IOException {
        clientProperties = Coder.readTable(in);
        mechanism = Coder.readShortString(in);
        response = Coder.readLongString(in);
        locale = Coder.readShortString(in);
    }

    protected void writeBody(BitSupportDataOutput out) throws IOException {
        Coder.writeTable(clientProperties, out);
        Coder.writeShortString(mechanism, out);
        Coder.writeLongString(response, out);
        Coder.writeShortString(locale, out);
        out.bitFlush();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer(" ");
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("clientProperties=");
        b.append(clientProperties);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("mechanism=");
        b.append(mechanism);
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("response=");
        if (response != null)
            b.append(new String(response));
        else
            b.append("null");
        if (!_first)
            b.append(", ");
        else
            _first = false;
        b.append("locale=");
        b.append(locale);
        return b.toString();
    }

    public String toString() {
        return "[StartOk " + super.toString() + getDisplayString() + "]";
    }
}
