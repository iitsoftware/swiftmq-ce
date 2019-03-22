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
import com.swiftmq.amqp.v100.types.AMQPString;

public class AMQPValueStringMessageFactory implements MessageFactory
{
  public AMQPMessage create(int sequenceNo) throws Exception
  {
    AMQPMessage msg = new AMQPMessage();
    msg.setAmqpValue(new AmqpValue(new AMQPString("Message #" + sequenceNo)));
    return msg;
  }

  public void verify(AMQPMessage message) throws Exception
  {
    AmqpValue value = message.getAmqpValue();
    if (value == null)
      throw new Exception(("verify - no AmqpValue section found!"));
    AMQPString s = (AMQPString) value.getValue();
    if (!s.getValue().startsWith("Message #"))
      throw new Exception("verify - invalid value detected: " + s.getValue());
  }

  public AMQPMessage createReplyMessage(AMQPMessage request) throws Exception
  {
    AMQPMessage reply = new AMQPMessage();
    reply.setAmqpValue(new AmqpValue(new AMQPString("RE: " + ((AMQPString) request.getAmqpValue().getValue()).getValue())));
    return reply;
  }
}
