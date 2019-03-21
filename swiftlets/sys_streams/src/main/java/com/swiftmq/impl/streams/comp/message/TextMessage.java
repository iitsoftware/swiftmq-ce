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
import com.swiftmq.jms.TextMessageImpl;

import javax.jms.JMSException;

/**
 * Facade to wrap a javax.jms.TextMessage.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class TextMessage extends Message {

    TextMessage(StreamContext ctx) {
        super(ctx, new TextMessageImpl());
    }

    TextMessage(StreamContext ctx, TextMessageImpl _impl) {
        super(ctx, _impl);
    }

  /**
   * return the type of this Message
   *
   * @return "text"
   */
  public String type() {
    return "text";
  }

    /**
     * Returns the body.
     *
     * @return body
     */
    public String body() {
        String s = null;
        try {
            s = ((TextMessageImpl) _impl).getText();
        } catch (JMSException e) {

        }
        return s;
    }

    /**
     * Sets the body.
     *
     * @param text body
     * @return TextMessage
     */
    public TextMessage body(String text) {
        ensureLocalCopy();
        try {
            ((TextMessageImpl) _impl).setText(text);
        } catch (JMSException e) {

        }
        return this;
    }
}
