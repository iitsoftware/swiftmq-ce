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

package com.swiftmq.amqp.v100.transport;

import com.swiftmq.amqp.v100.generated.transport.definitions.MessageFormat;
import com.swiftmq.amqp.v100.generated.transport.performatives.TransferFrame;
import com.swiftmq.amqp.v100.types.AMQPBoolean;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import java.io.IOException;

public class Packager
{
  int channel = 0;
  int handle = 0;
  boolean settled = false;
  byte[] data = null;
  int size = 0;
  DataByteArrayInputStream dbis = null;
  int maxPacketLength = 0;
  int currentPacketNumber = 0;
  long messageFormat = -1;
  int predictedNumberPackets = -1;

  public void setChannel(int channel)
  {
    this.channel = channel;
  }

  public void setHandle(int handle)
  {
    this.handle = handle;
  }

  public void setSettled(boolean settled)
  {
    this.settled = settled;
  }

  public void setData(byte[] data)
  {
    setData(data, data.length);
  }

  public void setData(byte[] data, int size)
  {
    this.data = data;
    this.size = size;
  }

  public byte[] getData()
  {
    return data;
  }

  public int getSize()
  {
    return size;
  }

  public long getMessageFormat()
  {
    return messageFormat;
  }

  public void setMessageFormat(long messageFormat)
  {
    this.messageFormat = messageFormat;
  }

  public void setMaxFrameSize(int maxFrameSize)
  {
    this.maxPacketLength = maxFrameSize;
  }

  public int getMaxPayloadLength()
  {
    return maxPacketLength;
  }

  public int getPredictedNumberPackets()
  {
    return predictedNumberPackets;
  }

  public void getNextPacket(TransferFrame currentFrame) throws IOException
  {
    currentFrame.setMore(AMQPBoolean.FALSE);
    if (messageFormat != -1)
      currentFrame.setMessageFormat(new MessageFormat(messageFormat));
    currentPacketNumber++;
    byte [] b = null;
    if (dbis != null)
    {
      int len = Math.min(dbis.available(), maxPacketLength-currentFrame.getPredictedSize());
      b = new byte[len];
      dbis.readFully(b);
    } else
    {
      if (maxPacketLength-currentFrame.getPredictedSize() - size >= 0)
      {
        if (data.length != size)
        {
          b = new byte[size];
          System.arraycopy(data, 0, b, 0, size);
          data = null;
        } else
          b = data;
      }
      else
      {
        if (dbis == null)
        {
          dbis = new DataByteArrayInputStream();
          dbis.setBuffer(data, 0, size);
        }
        int len = Math.min(dbis.available(), maxPacketLength-currentFrame.getPredictedSize());
        b = new byte[len];
        dbis.readFully(b);
      }
    }
    if (hasMore())
      currentFrame.setMore(AMQPBoolean.TRUE);
    currentFrame.setPayload(b);
    if (predictedNumberPackets == -1)
      predictedNumberPackets = size /b.length+1;
  }

  public int getCurrentPacketNumber()
  {
    return currentPacketNumber;
  }

  public boolean hasMore() throws IOException
  {
    return dbis != null && dbis.available() > 0;
  }
}
