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

package amqp.v100.base;

import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpSequence;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPLong;
import com.swiftmq.amqp.v100.types.AMQPString;
import com.swiftmq.amqp.v100.types.AMQPType;

import java.util.ArrayList;
import java.util.List;

public class AMQPSequenceMessageFactory implements MessageFactory {
    public AMQPMessage create(int sequenceNo) throws Exception {
        AMQPMessage msg = new AMQPMessage();
        List list = new ArrayList();
        list.add(new AMQPString("key1"));
        list.add(new AMQPLong(Integer.MAX_VALUE + 1));
        list.add(new AMQPString("key3"));
        list.add(new AMQPLong(Integer.MAX_VALUE + 2));
        msg.addAmqpSequence(new AmqpSequence(list));
        return msg;
    }

    public AMQPMessage createReplyMessage(AMQPMessage request) throws Exception {
        AMQPMessage reply = new AMQPMessage();
        List list = request.getAmqpSequence();
        ((AmqpSequence) list.get(0)).getValue().add(new AMQPString("REPLY"));
        for (int i = 0; i < list.size(); i++)
            reply.addAmqpSequence(((AmqpSequence) list.get(i)));
        return reply;
    }

    public void verify(AMQPMessage message) throws Exception {
        List list = message.getAmqpSequence();
        if (list == null)
            throw new Exception(("verify - no AmqpSequence section found!"));
        for (int i = 0; i < list.size(); i++) {
            List l2 = ((AmqpSequence) list.get(i)).getValue();
            for (int j = 0; j < l2.size(); j++) {
                AMQPType t = (AMQPType) l2.get(j);
                if (!((t instanceof AMQPString && ((AMQPString) t).getValue().equals("key1")) ||
                        (t instanceof AMQPString && ((AMQPString) t).getValue().equals("key3")) ||
                        (t instanceof AMQPString && ((AMQPString) t).getValue().equals("REPLY")) ||
                        (t instanceof AMQPString && ((AMQPString) t).getValue().equals(String.valueOf(Integer.MAX_VALUE + 1))) ||
                        (t instanceof AMQPString && ((AMQPString) t).getValue().equals(String.valueOf(Integer.MAX_VALUE + 2))) ||
                        (t instanceof AMQPLong && ((AMQPLong) t).getValue() == Integer.MAX_VALUE + 1) ||
                        (t instanceof AMQPLong && ((AMQPLong) t).getValue() == Integer.MAX_VALUE + 2))) {
                    System.out.println(l2);
                    throw new Exception("verify - invalid values in AmqpSequence detected: " + l2);
                }
            }
        }
    }
}
