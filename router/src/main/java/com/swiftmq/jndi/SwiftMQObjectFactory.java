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

package com.swiftmq.jndi;

import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TopicImpl;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

public class SwiftMQObjectFactory implements ObjectFactory {
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable environment) throws Exception {
        if (obj instanceof Reference) {
            Reference ref = (Reference) obj;
            if (ref.getClassName().equals(TopicImpl.class.getName())) {
                RefAddr addr = ref.get("topicName");
                if (addr != null) {
                    return new TopicImpl((String) addr.getContent());
                }
            } else if (ref.getClassName().equals(QueueImpl.class.getName())) {
                RefAddr addr = ref.get("queueName");
                if (addr != null) {
                    return new QueueImpl((String) addr.getContent());
                }
            }
        }
        return null;
    }
}
