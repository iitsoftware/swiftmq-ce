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

package com.swiftmq.amqp.v100.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Abstract base class for all AMQP types.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public abstract class AMQPType {
    String name;
    int code;
    static ThreadLocal writeCodeHolder = new ThreadLocal();
    AMQPDescribedConstructor constructor = null;
    AMQPDescribedConstructor originalConstructor = null;

    protected AMQPType(String name, int code) {
        this.name = name;
        this.code = code;
    }

    /**
     * Returns the constructor.
     *
     * @return constructor
     */
    public AMQPDescribedConstructor getConstructor() {
        return constructor;
    }

    /**
     * Sets the constructor.
     *
     * @param constructor constructor
     */
    public void setConstructor(AMQPDescribedConstructor constructor) {
        if (this.constructor != null)
            originalConstructor = this.constructor;
        this.constructor = constructor;
    }

    /**
     * Resets the constructor (AMQPValue anomaly; internal use only).
     */
    public void resetConstructor() {
        if (originalConstructor != null) {
            constructor = originalConstructor;
            originalConstructor = null;
        }
    }

    /**
     * Returns the name of the type.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the code of the type.
     *
     * @param code code
     */
    public void setCode(int code) {
        this.code = code;
    }

    /**
     * Returns the code of the type.
     *
     * @return code
     */
    public int getCode() {
        if (constructor != null)
            return AMQPTypeDecoder.CONSTRUCTOR;
        return code;
    }

    /**
     * Returns whether the code is written on writeContent
     *
     * @return true/false
     */
    public boolean isWriteCode() {
        Boolean b = (Boolean) writeCodeHolder.get();
        return b == null ? true : b.booleanValue();
    }

    /**
     * Sets whether the code should be written on writeContent (this will only be disabled for arrays)
     *
     * @param writeCode true/false
     */
    public void setWriteCode(boolean writeCode) {
        writeCodeHolder.set(Boolean.valueOf(writeCode));
    }

    /**
     * Returns the predicted size of this type. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        return 1 + (constructor == null ? 0 : constructor.getPredictedSize());
    }

    /**
     * Returns whether this type has a descriptor.
     *
     * @return true/false
     */
    public boolean hasDescriptor() {
        return false;
    }

    /**
     * Read the content of this type out of a DataInput.
     *
     * @param in DataInput
     * @throws IOException on error
     */
    public abstract void readContent(DataInput in) throws IOException;

    /**
     * Write the content of this type to a DataOutput.
     *
     * @param out DataOutput
     * @throws IOException on error
     */
    public void writeContent(DataOutput out) throws IOException {
        if (isWriteCode()) {
            if (constructor != null)
                constructor.writeContent(out);
            else
                out.writeByte(code);
        }
    }

    /**
     * Returns the value string representation of the type.
     *
     * @return value string
     */
    public abstract String getValueString();

    public String toString() {
        return " [AMQPType name=" + name + ", code=0x" + Integer.toHexString(code) + ", constructor=" + constructor + "]";
    }
}