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

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.jms.ObjectMessageImpl;

import javax.jms.JMSException;
import java.io.Serializable;

/**
 * Facade to wrap a javax.jms.ObjectMessage.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */

public class ObjectMessage extends Message {

    ObjectMessage(StreamContext ctx) {
        super(ctx, new ObjectMessageImpl());
    }

    ObjectMessage(StreamContext ctx, ObjectMessageImpl _impl) {
        super(ctx, _impl);
    }

  /**
   * return the type of this Message
   *
   * @return "object"
   */
  public String type() {
    return "object";
  }

    /**
     * Returns the body.
     *
     * @return Serializable
     */
    public Serializable body() {
        Serializable s = null;
        try {
            s = ((ObjectMessageImpl) _impl).getObject();
        } catch (JMSException e) {

        }
        return s;
    }

    /**
     * Sets the body.
     *
     * @param serializable body
     * @return ObjectMessage
     */
    public ObjectMessage body(Serializable serializable) {
        try {
            ((ObjectMessageImpl) _impl).setObject(serializable);
        } catch (JMSException e) {

        }
        return this;
    }
}
