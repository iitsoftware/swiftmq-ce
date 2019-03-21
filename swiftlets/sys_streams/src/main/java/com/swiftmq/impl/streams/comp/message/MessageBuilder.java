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
import com.swiftmq.jms.*;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

/**
 * Factory to create Message facades.
 * <p/>
 * A Message facade wraps a JMS message and provides a more elegant builder-style
 * access to it.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class MessageBuilder {
    StreamContext ctx;
    DataByteArrayOutputStream dbos = null;
    DataByteArrayInputStream dbis = null;

    /**
     * Internal use only.
     */
    public Message copyMessage(Message msg) throws Exception {
        if (dbos == null)
            dbos = new DataByteArrayOutputStream(4096);
        if (dbis == null)
            dbis = new DataByteArrayInputStream();
        MessageImpl impl = msg.getImpl();
        dbos.rewind();
        impl.writeContent(dbos);
        dbis.reset();
        dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
        MessageImpl msgCopy = MessageImpl.createInstance(dbis.readInt());
        msgCopy.readContent(dbis);
        return wrap(msgCopy);
    }

    /**
     * Internal use only.
     */
    public MessageBuilder(StreamContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Returns a new Message facade.
     *
     * @return Message facade
     */
    public Message message() {
        return new Message(ctx);
    }

    /**
     * Returns a new BytesMessage facade.
     *
     * @return BytesMessage facade
     */
    public BytesMessage bytesMessage() {
        return new BytesMessage(ctx);
    }

    /**
     * Returns a new TextMessage facade.
     *
     * @return TextMessage facade
     */
    public TextMessage textMessage() {
        return new TextMessage(ctx);
    }

    /**
     * Returns a new StreamMessage facade.
     *
     * @return StreamMessage facade
     */
    public StreamMessage streamMessage() {
        return new StreamMessage(ctx);
    }

    /**
     * Returns a new MapMessage facade.
     *
     * @return MapMessage facade
     */
    public MapMessage mapMessage() {
        return new MapMessage(ctx);
    }

    /**
     * Returns a new ObjectMessage facade.
     *
     * @return ObjectMessage facade
     */
    public ObjectMessage objectMessage() {
        return new ObjectMessage(ctx);
    }

    /**
     * Internal use only.
     */
    public Message wrap(MessageImpl impl) {
        if (impl instanceof BytesMessageImpl)
            return new BytesMessage(ctx, (BytesMessageImpl) impl);
        if (impl instanceof StreamMessageImpl)
            return new StreamMessage(ctx, (StreamMessageImpl) impl);
        if (impl instanceof TextMessageImpl)
            return new TextMessage(ctx, (TextMessageImpl) impl);
        if (impl instanceof ObjectMessageImpl)
            return new ObjectMessage(ctx, (ObjectMessageImpl) impl);
        if (impl instanceof MapMessageImpl)
            return new MapMessage(ctx, (MapMessageImpl) impl);
        return new Message(ctx, impl);
    }
}
