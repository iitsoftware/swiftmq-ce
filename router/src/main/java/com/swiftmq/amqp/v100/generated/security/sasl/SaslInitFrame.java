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

package com.swiftmq.amqp.v100.generated.security.sasl;

import com.swiftmq.amqp.v100.transport.AMQPFrame;
import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * Selects the sasl mechanism and provides the initial response if needed.
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class SaslInitFrame extends AMQPFrame
        implements SaslFrameIF {
    public static String DESCRIPTOR_NAME = "amqp:sasl-init:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000041L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    AMQPList body = null;
    boolean dirty = false;

    AMQPSymbol mechanism = null;
    AMQPBinary initialResponse = null;
    AMQPString hostname = null;

    /**
     * Constructs a SaslInitFrame.
     *
     * @param channel the channel id
     * @param body    the frame body
     */
    public SaslInitFrame(int channel, AMQPList body) throws Exception {
        super(channel);
        setTypeCode(TYPE_CODE_SASL_FRAME);
        this.body = body;
        if (body != null)
            decode();
    }

    /**
     * Constructs a SaslInitFrame.
     *
     * @param channel the channel id
     */
    public SaslInitFrame(int channel) {
        super(channel);
        setTypeCode(TYPE_CODE_SASL_FRAME);
    }

    /**
     * Accept method for a SaslFrame visitor.
     *
     * @param visitor SaslFrame visitor
     */
    public void accept(SaslFrameVisitor visitor) {
        visitor.visit(this);
    }


    /**
     * Sets the mandatory Mechanism field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * The name of the SASL mechanism used for the SASL exchange. If the selected mechanism is
     * not supported by the receiving peer, it MUST close the connection with the
     * authentication-failure close-code. Each peer MUST authenticate using the highest-level
     * security profile it can handle from the list provided by the partner.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param mechanism Mechanism
     */
    public void setMechanism(AMQPSymbol mechanism) {
        dirty = true;
        this.mechanism = mechanism;
    }

    /**
     * Returns the mandatory Mechanism field.
     *
     * @return Mechanism
     */
    public AMQPSymbol getMechanism() {
        return mechanism;
    }

    /**
     * Sets the optional InitialResponse field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * A block of opaque data passed to the security mechanism. The contents of this data are
     * defined by the SASL security mechanism.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param initialResponse InitialResponse
     */
    public void setInitialResponse(AMQPBinary initialResponse) {
        dirty = true;
        this.initialResponse = initialResponse;
    }

    /**
     * Returns the optional InitialResponse field.
     *
     * @return InitialResponse
     */
    public AMQPBinary getInitialResponse() {
        return initialResponse;
    }

    /**
     * Sets the optional Hostname field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * The DNS name of the host (either fully qualified or relative) to which the sending peer
     * is connecting. It is not mandatory to provide the hostname. If no hostname is provided
     * the receiving peer should select a default based on its own configuration.
     * </p><p>
     * </p><p>
     * This field can be used by AMQP proxies to determine the correct back-end service to
     * connect the client to, and to determine the domain to validate the client's credentials
     * against.
     * </p><p>
     * </p><p>
     * This field may already have been specified by the server name indication extension as
     * described in RFC-4366 [ RFC4366 ], if a TLS layer is used, in
     * which case this field SHOULD either be null or contain the same value. It is undefined
     * what a different value to those already specified means.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param hostname Hostname
     */
    public void setHostname(AMQPString hostname) {
        dirty = true;
        this.hostname = hostname;
    }

    /**
     * Returns the optional Hostname field.
     *
     * @return Hostname
     */
    public AMQPString getHostname() {
        return hostname;
    }


    /**
     * Returns the predicted size of this SaslInitFrame. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        int n = super.getPredictedSize();
        if (body == null) {
            n += 4; // For safety (length field and size of the list)
            n += codeConstructor.getPredictedSize();
            n += (mechanism != null ? mechanism.getPredictedSize() : 1);
            n += (initialResponse != null ? initialResponse.getPredictedSize() : 1);
            n += (hostname != null ? hostname.getPredictedSize() : 1);
        } else
            n += body.getPredictedSize();
        return n;
    }

    private AMQPArray singleToArray(AMQPType t) throws IOException {
        return new AMQPArray(t.getCode(), new AMQPType[]{t});
    }

    private void decode() throws Exception {
        List l = body.getValue();
        AMQPType t = null;
        int idx = 0;


        // Field: mechanism
        // Type     : symbol, converted: AMQPSymbol
        // Basetype : symbol
        // Default  : null
        // Mandatory: true
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() == AMQPTypeDecoder.NULL)
            throw new Exception("Mandatory field 'mechanism' in 'SaslInit' frame is NULL");
        try {
            mechanism = (AMQPSymbol) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'mechanism' in 'SaslInit' frame: " + e);
        }


        // Field: initialResponse
        // Type     : binary, converted: AMQPBinary
        // Basetype : binary
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                initialResponse = (AMQPBinary) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'initialResponse' in 'SaslInit' frame: " + e);
        }


        // Field: hostname
        // Type     : string, converted: AMQPString
        // Basetype : string
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                hostname = (AMQPString) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'hostname' in 'SaslInit' frame: " + e);
        }
    }

    private void addToList(List list, Object value) {
        if (value != null)
            list.add(value);
        else
            list.add(AMQPNull.NULL);
    }

    private void encode() throws IOException {
        List l = new ArrayList();
        addToList(l, mechanism);
        addToList(l, initialResponse);
        addToList(l, hostname);
        for (ListIterator iter = l.listIterator(l.size()); iter.hasPrevious(); ) {
            AMQPType t = (AMQPType) iter.previous();
            if (t.getCode() == AMQPTypeDecoder.NULL)
                iter.remove();
            else
                break;
        }
        body = new AMQPList(l);
        dirty = false;
    }

    protected void writeBody(DataOutput out) throws IOException {
        if (dirty || body == null)
            encode();
        codeConstructor.setFormatCode(body.getCode());
        body.setConstructor(codeConstructor);
        body.writeContent(out);
    }

    /**
     * Returns a value representation of this SaslInitFrame.
     *
     * @return value representation
     */
    public String getValueString() {
        try {
            if (dirty || body == null)
                encode();
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuffer b = new StringBuffer("[SaslInit ");
        b.append(super.getValueString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (mechanism != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("mechanism=");
            b.append(mechanism.getValueString());
        }
        if (initialResponse != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("initialResponse=");
            b.append(initialResponse.getValueString());
        }
        if (hostname != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("hostname=");
            b.append(hostname.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[SaslInit " + getDisplayString() + "]";
    }
}
