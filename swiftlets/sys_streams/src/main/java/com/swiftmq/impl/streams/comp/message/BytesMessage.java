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
import com.swiftmq.jms.BytesMessageImpl;

import javax.jms.JMSException;

/**
 * Facade to wrap a javax.jms.BytesMessage.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class BytesMessage extends Message {

    BytesMessage(StreamContext ctx) {
        super(ctx, new BytesMessageImpl());
    }

    BytesMessage(StreamContext ctx, BytesMessageImpl _impl) {
        super(ctx, _impl);
    }

    /**
     * return the type of this Message
     *
     * @return "bytes"
     */
    public String type() {
        return "bytes";
    }

    /**
     * Returns the body.
     *
     * @return body
     */
    public byte[] body() {
        byte[] b = null;
        try {
            ((BytesMessageImpl) _impl).reset();
            b = new byte[(int) ((BytesMessageImpl) _impl).getBodyLength()];
            ((BytesMessageImpl) _impl).readBytes(b);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return b;
    }

    /**
     * Sets the body.
     *
     * @param b body
     * @return BytesMessage
     */
    public BytesMessage body(byte[] b) {
        ensureLocalCopy();
        try {
            ((BytesMessageImpl) _impl).clearBody();
            ((BytesMessageImpl) _impl).writeBytes(b);
        } catch (JMSException e) {

        }
        return this;
    }
}
