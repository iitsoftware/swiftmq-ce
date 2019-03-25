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

package com.swiftmq.amqp.v100.generated.transport.performatives;

import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
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
 * Sending a close signals that the sender will not be sending any more frames (or bytes of
 * any other kind) on the connection. Orderly shutdown requires that this frame MUST be
 * written by the sender. It is illegal to send any more frames (or bytes of any other kind)
 * after sending a close frame.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class CloseFrame extends AMQPFrame
        implements FrameIF {
    public static String DESCRIPTOR_NAME = "amqp:close:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000018L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    AMQPList body = null;
    boolean dirty = false;

    Error error = null;

    /**
     * Constructs a CloseFrame.
     *
     * @param channel the channel id
     * @param body    the frame body
     */
    public CloseFrame(int channel, AMQPList body) throws Exception {
        super(channel);
        this.body = body;
        if (body != null)
            decode();
    }

    /**
     * Constructs a CloseFrame.
     *
     * @param channel the channel id
     */
    public CloseFrame(int channel) {
        super(channel);
    }

    /**
     * Accept method for a Frame visitor.
     *
     * @param visitor Frame visitor
     */
    public void accept(FrameVisitor visitor) {
        visitor.visit(this);
    }


    /**
     * Sets the optional Error field.
     *
     * <p>
     * </p><p>
     * </p><p>
     * If set, this field indicates that the connection is being closed due to an error
     * condition. The value of the field should contain details on the cause of the error.
     * </p><p>
     * </p><p>
     * </p><p>
     * </p>
     *
     * @param error Error
     */
    public void setError(Error error) {
        dirty = true;
        this.error = error;
    }

    /**
     * Returns the optional Error field.
     *
     * @return Error
     */
    public Error getError() {
        return error;
    }


    /**
     * Returns the predicted size of this CloseFrame. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        int n = super.getPredictedSize();
        if (body == null) {
            n += 4; // For safety (length field and size of the list)
            n += codeConstructor.getPredictedSize();
            n += (error != null ? error.getPredictedSize() : 1);
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


        // Field: error
        // Type     : Error, converted: Error
        // Basetype : list
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                error = new Error(((AMQPList) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'error' in 'Close' frame: " + e);
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
        addToList(l, error);
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
     * Returns a value representation of this CloseFrame.
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
        StringBuffer b = new StringBuffer("[Close ");
        b.append(super.getValueString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (error != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("error=");
            b.append(error.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Close " + getDisplayString() + "]";
    }
}
