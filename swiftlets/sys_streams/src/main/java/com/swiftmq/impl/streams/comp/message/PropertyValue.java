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

package com.swiftmq.impl.streams.comp.message;

/**
 * Facade to wrap the Value of a Message Property
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class PropertyValue {
    Object value;

    PropertyValue(Object value) {
        this.value = value;
    }

    Object value() {
        return value;
    }

    /**
     * Converts the value to boolean.
     *
     * @return Boolean
     */
    public Boolean toBoolean() {
        return new Boolean(value.toString());
    }

    /**
     * Converts the value to byte.
     *
     * @return Byte
     */
    public Byte toByte() {
        return new Byte(value.toString());
    }

    /**
     * Converts the value to integer.
     *
     * @return Integer
     */
    public Integer toInteger() {
        return toDouble().intValue();
    }

    /**
     * Converts the value to double.
     *
     * @return Double
     */
    public Double toDouble() {
        return new Double(value.toString());
    }

    /**
     * Converts the value to float.
     *
     * @return Float
     */
    public Float toFloat() {
        return new Float(value.toString());
    }

    /**
     * Converts the value to long.
     *
     * @return Long
     */
    public Long toLong() {
        return toDouble().longValue();
    }

    /**
     * Returns the value as is.
     *
     * @return Object
     */
    public Object toObject() {
        return value;
    }

    /**
     * Converts the value to string.
     *
     * @return String
     */
    public String toString() {
        return value.toString();
    }
}
