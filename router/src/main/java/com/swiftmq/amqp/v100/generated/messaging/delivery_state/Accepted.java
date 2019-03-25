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

package com.swiftmq.amqp.v100.generated.messaging.delivery_state;

import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * </p><p>
 * At the source the accepted state means that the message has been retired from the node,
 * and transfer of payload data will not be able to be resumed if the link becomes suspended.
 * A delivery may become accepted at the source even before all transfer frames have been
 * sent, this does not imply that the remaining transfers for the delivery will not be sent -
 * only the aborted flag on the   performative can be used
 * to indicate a premature termination of the transfer.
 * </p><p>
 * </p><p>
 * At the target, the accepted outcome is used to indicate that an incoming message has been
 * successfully processed, and that the receiver of the message is expecting the sender to
 * transition the delivery to the accepted state at the source.
 * </p><p>
 * </p><p>
 * The accepted outcome does not increment the  delivery-count  in the header of the
 * accepted message.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Accepted extends AMQPList
        implements DeliveryStateIF, OutcomeIF {
    public static String DESCRIPTOR_NAME = "amqp:accepted:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000024L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    boolean dirty = false;


    /**
     * Constructs a Accepted.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Accepted(List initValue) throws Exception {
        super(initValue);
        if (initValue != null)
            decode();
    }

    /**
     * Constructs a Accepted.
     */
    public Accepted() {
        dirty = true;
    }

    /**
     * Return whether this Accepted has a descriptor
     *
     * @return true/false
     */
    public boolean hasDescriptor() {
        return true;
    }

    /**
     * Accept method for a DeliveryState visitor.
     *
     * @param visitor DeliveryState visitor
     */
    public void accept(DeliveryStateVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Accept method for a Outcome visitor.
     *
     * @param visitor Outcome visitor
     */
    public void accept(OutcomeVisitor visitor) {
        visitor.visit(this);
    }


    /**
     * Returns the predicted size of this Accepted. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        int n;
        if (dirty) {
            AMQPDescribedConstructor _c = getConstructor();
            setConstructor(null);
            n = super.getPredictedSize();
            n += codeConstructor.getPredictedSize();
            setConstructor(_c);
        } else
            n = super.getPredictedSize();
        return n;
    }

    private AMQPArray singleToArray(AMQPType t) throws IOException {
        return new AMQPArray(t.getCode(), new AMQPType[]{t});
    }

    private void decode() throws Exception {
        List l = getValue();

        AMQPType t = null;
        int idx = 0;
    }

    private void addToList(List list, Object value) {
        if (value != null)
            list.add(value);
        else
            list.add(AMQPNull.NULL);
    }

    private void encode() throws IOException {
        List l = new ArrayList();
        for (ListIterator iter = l.listIterator(l.size()); iter.hasPrevious(); ) {
            AMQPType t = (AMQPType) iter.previous();
            if (t.getCode() == AMQPTypeDecoder.NULL)
                iter.remove();
            else
                break;
        }
        setValue(l);
        dirty = false;
    }

    /**
     * Returns an array constructor (internal use)
     *
     * @return array constructor
     */
    public AMQPDescribedConstructor getArrayConstructor() throws IOException {
        if (dirty)
            encode();
        codeConstructor.setFormatCode(getCode());
        return codeConstructor;
    }

    public void writeContent(DataOutput out) throws IOException {
        if (dirty)
            encode();
        if (getConstructor() != codeConstructor) {
            codeConstructor.setFormatCode(getCode());
            setConstructor(codeConstructor);
        }
        super.writeContent(out);
    }

    public String getValueString() {
        try {
            if (dirty)
                encode();
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuffer b = new StringBuffer("[Accepted ");
        b.append(getDisplayString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        return b.toString();
    }

    public String toString() {
        return "[Accepted " + getDisplayString() + "]";
    }
}
