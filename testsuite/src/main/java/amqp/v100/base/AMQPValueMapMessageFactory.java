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

import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPLong;
import com.swiftmq.amqp.v100.types.AMQPMap;
import com.swiftmq.amqp.v100.types.AMQPString;
import com.swiftmq.amqp.v100.types.AMQPType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AMQPValueMapMessageFactory implements MessageFactory
{
  public AMQPMessage create(int sequenceNo) throws Exception
  {
    AMQPMessage msg = new AMQPMessage();
    Map map = new HashMap();
    map.put(new AMQPString("key1"), new AMQPString("value1"));
    map.put(new AMQPLong(Integer.MAX_VALUE + 1), new AMQPLong(200));
    map.put(new AMQPString("key3"), new AMQPString("value1"));
    map.put(new AMQPLong(Integer.MAX_VALUE + 2), new AMQPLong(400));
    msg.setAmqpValue(new AmqpValue(new AMQPMap(map)));
    return msg;
  }

  public AMQPMessage createReplyMessage(AMQPMessage request) throws Exception
  {
    AMQPMessage reply = new AMQPMessage();
    Map map = ((AMQPMap) request.getAmqpValue().getValue()).getValue();
    map.put(new AMQPString("REPLY"), new AMQPString("REPLY"));
    reply.setAmqpValue(new AmqpValue(new AMQPMap(map)));
    return reply;
  }

  public void verify(AMQPMessage message) throws Exception
  {
    AmqpValue value = message.getAmqpValue();
    if (value == null)
      throw new Exception(("verify - no AmqpValue section found!"));
    AMQPType t = value.getValue();
    if (!(t instanceof AMQPMap))
      throw new Exception(("verify - AmqpValue does not contain an AmqpMap!"));
    Map map = ((AMQPMap) message.getAmqpValue().getValue()).getValue();
    for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); )
    {
      Map.Entry entry = (Map.Entry) iter.next();
      if (!((entry.getKey() instanceof AMQPString && ((AMQPString) entry.getKey()).getValue().equals("key1")) ||
          (entry.getKey() instanceof AMQPString && ((AMQPString) entry.getKey()).getValue().equals("key3")) ||
          (entry.getKey() instanceof AMQPString && ((AMQPString) entry.getKey()).getValue().equals("REPLY")) ||
          (entry.getKey() instanceof AMQPString && ((AMQPString) entry.getKey()).getValue().equals(String.valueOf(Integer.MAX_VALUE + 1))) ||
          (entry.getKey() instanceof AMQPString && ((AMQPString) entry.getKey()).getValue().equals(String.valueOf(Integer.MAX_VALUE + 2))) ||
          (entry.getKey() instanceof AMQPLong && ((AMQPLong) entry.getKey()).getValue() == Integer.MAX_VALUE + 1) ||
          (entry.getKey() instanceof AMQPLong && ((AMQPLong) entry.getKey()).getValue() == Integer.MAX_VALUE + 2)))
      {
        System.out.println(map);
        throw new Exception("verify - invalid values in map detected: " + map);
      }

    }
  }
}
