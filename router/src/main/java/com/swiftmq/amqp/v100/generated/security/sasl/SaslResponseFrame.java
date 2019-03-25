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
 * Send the SASL response data as defined by the SASL specification.
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class SaslResponseFrame extends AMQPFrame
        implements SaslFrameIF {
    public static String DESCRIPTOR_NAME = "amqp:sasl-response:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000043L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    AMQPList body = null;
    boolean dirty = false;

    AMQPBinary response = null;

    /**
     * Constructs a SaslResponseFrame.
     *
     * @param channel the channel id
     * @param body    the frame body
     */
    public SaslResponseFrame(int channel, AMQPList body) throws Exception {
        super(channel);
        setTypeCode(TYPE_CODE_SASL_FRAME);
        this.body = body;
        if (body != null)
            decode();
    }

    /**
     * Constructs a SaslResponseFrame.
     *
     * @param channel the channel id
     */
    public SaslResponseFrame(int channel) {
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
     * Sets the mandatory Response field.
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
     * @param response Response
     */
    public void setResponse(AMQPBinary response) {
        dirty = true;
        this.response = response;
    }

    /**
     * Returns the mandatory Response field.
     *
     * @return Response
     */
    public AMQPBinary getResponse() {
        return response;
    }


    /**
     * Returns the predicted size of this SaslResponseFrame. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        int n = super.getPredictedSize();
        if (body == null) {
            n += 4; // For safety (length field and size of the list)
            n += codeConstructor.getPredictedSize();
            n += (response != null ? response.getPredictedSize() : 1);
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


        // Field: response
        // Type     : binary, converted: AMQPBinary
        // Basetype : binary
        // Default  : null
        // Mandatory: true
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() == AMQPTypeDecoder.NULL)
            throw new Exception("Mandatory field 'response' in 'SaslResponse' frame is NULL");
        try {
            response = (AMQPBinary) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'response' in 'SaslResponse' frame: " + e);
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
        addToList(l, response);
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
     * Returns a value representation of this SaslResponseFrame.
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
        StringBuffer b = new StringBuffer("[SaslResponse ");
        b.append(super.getValueString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (response != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("response=");
            b.append(response.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[SaslResponse " + getDisplayString() + "]";
    }
}
