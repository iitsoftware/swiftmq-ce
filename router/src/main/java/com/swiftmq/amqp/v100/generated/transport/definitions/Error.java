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

package com.swiftmq.amqp.v100.generated.transport.definitions;

import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Error extends AMQPList {
    public static String DESCRIPTOR_NAME = "amqp:error:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x0000001dL;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    boolean dirty = false;

    ErrorConditionIF condition = null;
    AMQPString description = null;
    Fields info = null;

    /**
     * Constructs a Error.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Error(List initValue) throws Exception {
        super(initValue);
        if (initValue != null)
            decode();
    }

    /**
     * Constructs a Error.
     */
    public Error() {
        dirty = true;
    }

    /**
     * Return whether this Error has a descriptor
     *
     * @return true/false
     */
    public boolean hasDescriptor() {
        return true;
    }

    /**
     * Returns the mandatory Condition field.
     *
     * @return Condition
     */
    public ErrorConditionIF getCondition() {
        return condition;
    }

    /**
     * Sets the mandatory Condition field.
     *
     * @param condition Condition
     */
    public void setCondition(ErrorConditionIF condition) {
        dirty = true;
        this.condition = condition;
    }

    /**
     * Returns the optional Description field.
     *
     * @return Description
     */
    public AMQPString getDescription() {
        return description;
    }

    /**
     * Sets the optional Description field.
     *
     * @param description Description
     */
    public void setDescription(AMQPString description) {
        dirty = true;
        this.description = description;
    }

    /**
     * Returns the optional Info field.
     *
     * @return Info
     */
    public Fields getInfo() {
        return info;
    }

    /**
     * Sets the optional Info field.
     *
     * @param info Info
     */
    public void setInfo(Fields info) {
        dirty = true;
        this.info = info;
    }

    /**
     * Returns the predicted size of this Error. The predicted size may be greater than the actual size
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
            n += (condition != null ? condition.getPredictedSize() : 1);
            n += (description != null ? description.getPredictedSize() : 1);
            n += (info != null ? info.getPredictedSize() : 1);
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

        // Field: condition
        // Type     : ErrorConditionIF, converted: ErrorConditionIF
        // Basetype : ErrorConditionIF
        // Default  : null
        // Mandatory: true
        // Multiple : false
        // Factory  : ErrorConditionFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() == AMQPTypeDecoder.NULL)
            throw new Exception("Mandatory field 'condition' in 'Error' type is NULL");
        condition = ErrorConditionFactory.create(t);

        // Field: description
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
                description = (AMQPString) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'description' in 'Error' type: " + e);
        }

        // Field: info
        // Type     : Fields, converted: Fields
        // Basetype : map
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                info = new Fields(((AMQPMap) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'info' in 'Error' type: " + e);
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
        addToList(l, condition);
        addToList(l, description);
        addToList(l, info);
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
        StringBuffer b = new StringBuffer("[Error ");
        b.append(getDisplayString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (condition != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("condition=");
            b.append(condition.getValueString());
        }
        if (description != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("description=");
            b.append(description.getValueString());
        }
        if (info != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("info=");
            b.append(info.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Error " + getDisplayString() + "]";
    }
}
