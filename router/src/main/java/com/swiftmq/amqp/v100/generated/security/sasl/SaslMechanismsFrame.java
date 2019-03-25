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
 * </p><p>
 * Advertises the available SASL mechanisms that may be used for authentication.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class SaslMechanismsFrame extends AMQPFrame
        implements SaslFrameIF {
    public static String DESCRIPTOR_NAME = "amqp:sasl-mechanisms:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000040L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    AMQPList body = null;
    boolean dirty = false;

    AMQPArray saslServerMechanisms = null;

    /**
     * Constructs a SaslMechanismsFrame.
     *
     * @param channel the channel id
     * @param body    the frame body
     */
    public SaslMechanismsFrame(int channel, AMQPList body) throws Exception {
        super(channel);
        setTypeCode(TYPE_CODE_SASL_FRAME);
        this.body = body;
        if (body != null)
            decode();
    }

    /**
     * Constructs a SaslMechanismsFrame.
     *
     * @param channel the channel id
     */
    public SaslMechanismsFrame(int channel) {
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
     * Sets the mandatory SaslServerMechanisms field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * A list of the sasl security mechanisms supported by the sending peer. It is invalid
     * for this list to be null or empty. If the sending peer does not require its partner
     * to authenticate with it, then it should send a list of one element with its value as
     * the SASL mechanism  ANONYMOUS . The server mechanisms are ordered in decreasing
     * level of preference.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param saslServerMechanisms SaslServerMechanisms
     */
    public void setSaslServerMechanisms(AMQPArray saslServerMechanisms) {
        dirty = true;
        this.saslServerMechanisms = saslServerMechanisms;
    }

    /**
     * Returns the mandatory SaslServerMechanisms field.
     *
     * @return SaslServerMechanisms
     */
    public AMQPArray getSaslServerMechanisms() {
        return saslServerMechanisms;
    }


    /**
     * Returns the predicted size of this SaslMechanismsFrame. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        int n = super.getPredictedSize();
        if (body == null) {
            n += 4; // For safety (length field and size of the list)
            n += codeConstructor.getPredictedSize();
            n += (saslServerMechanisms != null ? saslServerMechanisms.getPredictedSize() : 1);
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


        // Field: saslServerMechanisms
        // Type     : symbol, converted: AMQPSymbol
        // Basetype : symbol
        // Default  : null
        // Mandatory: true
        // Multiple : true
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() == AMQPTypeDecoder.NULL)
            throw new Exception("Mandatory field 'saslServerMechanisms' in 'SaslMechanisms' frame is NULL");
        try {
            saslServerMechanisms = AMQPTypeDecoder.isArray(t.getCode()) ? (AMQPArray) t : singleToArray(t);
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'saslServerMechanisms' in 'SaslMechanisms' frame: " + e);
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
        addToList(l, saslServerMechanisms);
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
     * Returns a value representation of this SaslMechanismsFrame.
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
        StringBuffer b = new StringBuffer("[SaslMechanisms ");
        b.append(super.getValueString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (saslServerMechanisms != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("saslServerMechanisms=");
            b.append(saslServerMechanisms.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[SaslMechanisms " + getDisplayString() + "]";
    }
}
