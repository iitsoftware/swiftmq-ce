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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Facade to wrap the body of a javax.jms.MapMessage
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class MapBody {
    MapMessage message;
    MapMessageImpl impl;

    MapBody(MapMessage message) {
        this.message = message;
        this.impl = (MapMessageImpl) message.getImpl();
    }

    /**
     * Returns an array of names (keys) of the map.
     *
     * @return array of names
     * @throws JMSException
     */
    public String[] names() throws JMSException {
        List<String> list = new ArrayList<String>();
        for (Enumeration<String> _enum = impl.getMapNames(); _enum.hasMoreElements(); )
            list.add(_enum.nextElement());
        return list.toArray(new String[list.size()]);
    }

    /**
     * Get a map value.
     *
     * @param name key
     * @return value
     */
    public MapValue get(String name) {
        return new MapValue(impl, name);
    }

    /**
     * Sets a map value.
     *
     * @param name  key
     * @param value value
     * @return MapBody
     * @throws JMSException
     */
    public MapBody set(String name, Object value) throws JMSException {
        message.ensureLocalCopy();
        impl = (MapMessageImpl) message.getImpl();
        impl.setObject(name, value);
        return this;
    }

    private String jsonValue(Object o) throws Exception {
        if (o instanceof Number)
            return String.valueOf(o);
        return new StringBuilder("\"").append(o.toString().replace("\"", "\\\"").replace("\n", "\\n").replace("\t", "\\t")).append("\"").toString();
    }

    /**
     * Converts this body into a Json
     *
     * @return json
     * @throws Exception
     */
    public String toJson() throws Exception {
        StringBuilder b = new StringBuilder("{");
        String[] names = names();
        for (int i = 0; i < names.length; i++) {
            b.append("\"");
            b.append(names[i]);
            b.append("\":");
            b.append(jsonValue(get(names[i]).value()));
            if (i < names.length - 1)
                b.append(",");
        }
        b.append("}");
        return b.toString();
    }

    /**
     * Returns a reference to the message.
     *
     * @return MapMessage
     */
    public MapMessage message() {
        return message;
    }

    /**
     * Clears the body.
     *
     * @return MapBody
     * @throws JMSException
     */
    public MapBody clear() throws JMSException {
        message.ensureLocalCopy();
        impl = (MapMessageImpl) message.getImpl();
        impl.clearBody();
        return this;
    }
}
