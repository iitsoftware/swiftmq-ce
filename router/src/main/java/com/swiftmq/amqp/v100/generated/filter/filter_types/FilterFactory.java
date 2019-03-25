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

package com.swiftmq.amqp.v100.generated.filter.filter_types;

import com.swiftmq.amqp.v100.types.*;

import java.io.IOException;

/**
 * Factory class to create FilterIF objects out of a bare AMQPType
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class FilterFactory {

    /**
     * Creates a FilterIF object.
     *
     * @param bare the bare AMQP type
     * @return FilterIF
     */
    public static FilterIF create(AMQPType bare) throws Exception {
        if (bare.getCode() == AMQPTypeDecoder.NULL)
            return null;
        AMQPDescribedConstructor constructor = bare.getConstructor();
        if (constructor == null)
            throw new IOException("Missing constructor: " + bare);
        AMQPType descriptor = constructor.getDescriptor();
        int code = descriptor.getCode();
        if (AMQPTypeDecoder.isULong(code)) {
            long type = ((AMQPUnsignedLong) descriptor).getValue();
            if (type == NoLocalFilter.DESCRIPTOR_CODE)
                return new NoLocalFilter(((AMQPList) bare).getValue());
            if (type == SelectorFilter.DESCRIPTOR_CODE)
                return new SelectorFilter(((AMQPString) bare).getValue());
            throw new Exception("Invalid descriptor type: " + type + ", bare=" + bare);
        } else if (AMQPTypeDecoder.isSymbol(code)) {
            String type = ((AMQPSymbol) descriptor).getValue();
            if (type.equals(NoLocalFilter.DESCRIPTOR_NAME))
                return new NoLocalFilter(((AMQPList) bare).getValue());
            if (type.equals(SelectorFilter.DESCRIPTOR_NAME))
                return new SelectorFilter(((AMQPString) bare).getValue());
            throw new Exception("Invalid descriptor type: " + type + ", bare=" + bare);
        } else
            throw new Exception("Invalid type of constructor descriptor (actual type=" + code + ", expected=symbold or ulong), bare= " + bare);
    }

    /**
     * Converts an AMQP array of type FilterIF into a native array
     *
     * @param array AMQP array
     * @return native array
     */
    public static FilterIF[] toNativeArray(AMQPArray array) throws Exception {
        if (array == null)
            return null;
        AMQPType[] value = array.getValue();
        if (value == null)
            return null;
        FilterIF[] n = new FilterIF[value.length];
        for (int i = 0; i < value.length; i++)
            n[i] = create(value[i]);
        return n;
    }
}
