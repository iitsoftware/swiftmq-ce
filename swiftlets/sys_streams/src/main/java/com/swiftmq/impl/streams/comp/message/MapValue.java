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

import com.swiftmq.jms.MapMessageImpl;

import javax.jms.JMSException;

/**
 * Facade to wrap the Value of a Map Body
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class MapValue {
    MapMessageImpl message;
    String name;

    MapValue(MapMessageImpl message, String name) {
        this.message = message;
        this.name = name;
    }

    /**
     * Returns the value as Object.
     *
     * @return Object
     * @throws JMSException
     */
    public Object value() throws JMSException {
        return message.getObject(name);
    }

    /**
     * Converts the value to Byte.
     *
     * @return Byte
     * @throws JMSException
     */
    public Byte toByte() throws JMSException {
        return message.getByte(name);
    }

    /**
     * Converts the value to byte[].
     * @return byte[]
     * @throws JMSException
     */
    public byte[] toBytes() throws JMSException {
        return message.getBytes(name);
    }

    /**
     * Converts the value to Short.
     * @return Short
     * @throws JMSException
     */
    public Short toShort() throws JMSException {
        return message.getShort(name);
    }

    /**
     * Converts the value to Integer.
     * @return Integer
     * @throws JMSException
     */
    public Integer toInteger() throws JMSException {
        return message.getInt(name);
    }

    /**
     * Converts the value to Double.
     * @return Double
     * @throws JMSException
     */
    public Double toDouble() throws JMSException {
        return message.getDouble(name);
    }

    /**
     * Converts the value to Float.
     * @return Float
     * @throws JMSException
     */
    public Float toFloat() throws JMSException {
        return message.getFloat(name);
    }

    /**
     * Converts the value to Long.
     * @return Long
     * @throws JMSException
     */
    public Long toLong() throws JMSException {
        return message.getLong(name);
    }

    /**
     * Converts the value to String.
     * @return String
     * @throws JMSException
     */
    public String toStringValue() throws JMSException {
        return value().toString();
    }
}
