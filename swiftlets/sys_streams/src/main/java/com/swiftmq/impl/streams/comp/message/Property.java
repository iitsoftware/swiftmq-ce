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
 * Facade to wrap the Property of a javax.jms.Message
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class Property {
    Message message = null;
    String name;
    PropertyValue value;

    /**
     * Internal use.
     */
    public Property(Message message, String name, Object value) {
        this.message = message;
        this.name = name;
        this.value = new PropertyValue(value);
    }

    /**
     * Returns the name of this property.
     *
     * @return name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the value.
     *
     * @return PropertyValue
     */
    public PropertyValue value() {
        return value;
    }

    /**
     * Sets the value.
     *
     * @param value value
     * @return Message
     */
    public Message set(Object value) {
        this.value = new PropertyValue(value);
        message.addProperty(this);
        return message;
    }

    /**
     * Returns whether this Property exists in the Message
     *
     * @return exists or not
     */
    public boolean exists() {
        return value.toObject() != null;
    }

    /**
     * Clears the property value.
     *
     * @return Message
     */
    public Message clear() {
        value = null;
        message.removeProperty(this);
        return message;
    }
}
