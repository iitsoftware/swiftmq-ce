/*
 * Copyright 2020 IIT Software GmbH
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

package com.swiftmq.impl.streams;

/**
 * Convenience class to convert an object into various basic types. It is accessible from
 * Streams scripts via variable "typeconvert".
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2020, All Rights Reserved
 */
public class TypeConverter {

    /**
     * Convert an object to Boolean
     *
     * @param o Object
     * @return Boolean
     */
    public boolean toBoolean(Object o) {
        return o != null && o.toString().equals("true");
    }

    /**
     * Convert an object to Integer
     *
     * @param o Object
     * @return Integer
     */
    public int toInteger(Object o) throws Exception {
        if (o == null)
            throw new NullPointerException();
        if (o instanceof String)
            return Integer.parseInt((String) o);
        if (o instanceof Boolean)
            throw new Exception("Can't convert Boolean to Integer!");
        if (o instanceof Integer)
            return (Integer) o;
        if (o instanceof Long)
            return ((Long) o).intValue();
        if (o instanceof Double)
            return ((Double) o).intValue();
        if (o instanceof Float)
            return ((Float) o).intValue();
        throw new Exception("Can convert this type to Integer: " + o.getClass().getName());
    }

    /**
     * Convert an object to Long
     *
     * @param o Object
     * @return Long
     */
    public long toLong(Object o) throws Exception {
        if (o == null)
            throw new NullPointerException();
        if (o instanceof String)
            return Long.parseLong((String) o);
        if (o instanceof Boolean)
            throw new Exception("Can't convert Boolean to Long!");
        if (o instanceof Integer)
            return ((Integer) o).longValue();
        if (o instanceof Long)
            return (Long) o;
        if (o instanceof Double)
            return ((Double) o).longValue();
        if (o instanceof Float)
            return ((Float) o).longValue();
        throw new Exception("Can convert this type to Long: " + o.getClass().getName());
    }

    /**
     * Convert an object to Double
     *
     * @param o Object
     * @return Double
     */
    public double toDouble(Object o) throws Exception {
        if (o == null)
            throw new NullPointerException();
        if (o instanceof String)
            return Double.parseDouble((String) o);
        if (o instanceof Boolean)
            throw new Exception("Can't convert Boolean to Double!");
        if (o instanceof Integer)
            return ((Integer) o).doubleValue();
        if (o instanceof Long)
            return ((Long) o).doubleValue();
        if (o instanceof Double)
            return (Double) o;
        if (o instanceof Float)
            return ((Float) o).doubleValue();
        throw new Exception("Can convert this type to Double: " + o.getClass().getName());
    }

    /**
     * Convert an object to Float
     *
     * @param o Object
     * @return Float
     */
    public double toFloat(Object o) throws Exception {
        if (o == null)
            throw new NullPointerException();
        if (o instanceof String)
            return Float.parseFloat((String) o);
        if (o instanceof Boolean)
            throw new Exception("Can't convert Boolean to Float!");
        if (o instanceof Integer)
            return ((Integer) o).floatValue();
        if (o instanceof Long)
            return ((Long) o).floatValue();
        if (o instanceof Double)
            return ((Double) o).floatValue();
        if (o instanceof Float)
            return (Float) o;
        throw new Exception("Can convert this type to Float: " + o.getClass().getName());
    }
}
