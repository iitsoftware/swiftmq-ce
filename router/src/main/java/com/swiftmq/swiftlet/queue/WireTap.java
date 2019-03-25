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

package com.swiftmq.swiftlet.queue;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WireTap {
    String name;
    List<WireTapSubscriber> subscribers = new ArrayList<WireTapSubscriber>();
    int nextSubscriber = 0;

    public WireTap(String name, WireTapSubscriber subscriber) {
        this.name = name;
        subscribers.add(subscriber);
    }

    public String getName() {
        return name;
    }

    public void addSubscriber(WireTapSubscriber subscriber) {
        subscribers.add(subscriber);
    }

    public void removeSubscriber(WireTapSubscriber subscriber) {
        subscribers.remove(subscriber);
    }

    public boolean hasSubscribers() {
        return subscribers.size() > 0;
    }

    private MessageImpl copyMessage(MessageImpl msg) {
        try {
            DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
            DataByteArrayInputStream dbis = new DataByteArrayInputStream();
            msg.writeContent(dbos);
            dbis.reset();
            dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
            MessageImpl msgCopy = MessageImpl.createInstance(dbis.readInt());
            msgCopy.readContent(dbis);
            return msgCopy;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void putMessage(MessageImpl message) {
        if (!hasSubscribers())
            return;
        if (nextSubscriber >= subscribers.size())
            nextSubscriber = 0;
        WireTapSubscriber subscriber = subscribers.get(nextSubscriber);
        if (subscriber.isSelected(message)) {
            if (subscriber.requieresDeepCopy())
                subscriber.putMessage(copyMessage(message));
            else
                subscriber.putMessage(message);
        }
        nextSubscriber++;
    }
}
