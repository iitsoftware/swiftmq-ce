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

package com.swiftmq.impl.amqp.amqp.v01_00_00.transformer;

import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpSequence;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Data;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.jms.*;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BodyTypeMessageFactory implements MessageFactory
{

  public MessageImpl create(AMQPMessage source) throws Exception
  {
    List data = source.getData();
    if (data != null && data.size() > 0)
    {
      if (data.size() == 1)
      {
        byte[] b = ((Data) data.get(0)).getValue();
        return new BytesMessageImpl(b, b.length);
      } else
      {
        BytesMessageImpl msg = new BytesMessageImpl();
        for (int i = 0; i < data.size(); i++)
        {
          msg.writeBytes(((Data) data.get(i)).getValue());
        }
        return msg;
      }
    }

    List sequenceList = source.getAmqpSequence();
    if (sequenceList != null && sequenceList.size() > 0)
    {
      StreamMessageImpl msg = new StreamMessageImpl();
      for (int i = 0; i < sequenceList.size(); i++)
      {
        AmqpSequence seq = (AmqpSequence) sequenceList.get(i);
        List list = seq.getValue();
        if (list != null && list.size() > 0)
        {
          for (int j = 0; j < list.size(); j++)
          {
            try
            {
              msg.writeObject(Util.convertAMQPtoJMS((AMQPType) list.get(j)));
            } catch (Exception e)
            {
              msg.setBooleanProperty(PROP_ERROR, true);
              msg.setStringProperty(PROP_ERROR_CAUSE, e.toString());
              break;
            }
          }
        }
      }
      return msg;
    }

    AmqpValue amqpValue = source.getAmqpValue();
    if (amqpValue != null)
    {
      AMQPType value = amqpValue.getValue();
      AMQPDescribedConstructor constructor = value.getConstructor();
      int code = constructor != null ? constructor.getFormatCode() : value.getCode();
      if (AMQPTypeDecoder.isSymbol(code) || AMQPTypeDecoder.isString(code))
      {
        TextMessageImpl msg = new TextMessageImpl();
        msg.setText((String) Util.convertAMQPtoJMS(value));
        return msg;
      }

      if (AMQPTypeDecoder.isBinary(code))
      {
        byte[] b = ((AMQPBinary) value).getValue();
        return new BytesMessageImpl(b, b.length);
      }

      if (AMQPTypeDecoder.isList(code))
      {
        StreamMessageImpl msg = new StreamMessageImpl();
        List list = ((AMQPList) value).getValue();
        for (int i = 0; i < list.size(); i++)
        {
          try
          {
            msg.writeObject(Util.convertAMQPtoJMS((AMQPType) list.get(i)));
          } catch (Exception e)
          {
            msg.setBooleanProperty(PROP_ERROR, true);
            msg.setStringProperty(PROP_ERROR_CAUSE, e.toString());
            break;
          }
        }
        return msg;
      }

      if (AMQPTypeDecoder.isMap(code))
      {
        MapMessageImpl msg = new MapMessageImpl();
        Map map = ((AMQPMap) value).getValue();
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); )
        {
          Map.Entry entry = (Map.Entry) iter.next();
          try
          {
            String name = Util.convertAMQPtoJMS((AMQPType) entry.getKey()).toString();
            msg.setObject(name, Util.convertAMQPtoJMS((AMQPType) entry.getValue()));
          } catch (Exception e)
          {
            msg.setBooleanProperty(PROP_ERROR, true);
            msg.setStringProperty(PROP_ERROR_CAUSE, e.toString());
            break;
          }
        }
        return msg;
      }

      // Everything else is a ObjectMessage
      ObjectMessageImpl msg = new ObjectMessageImpl();
      try
      {
        msg.setObject((Serializable) Util.convertAMQPtoJMS(value));
      } catch (Exception e)
      {
        msg.setBooleanProperty(PROP_ERROR, true);
        msg.setStringProperty(PROP_ERROR_CAUSE, e.toString());
      }
      return msg;

    }

    return new MessageImpl();
  }
}
