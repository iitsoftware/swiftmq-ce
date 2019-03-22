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

package com.swiftmq.filetransfer.protocol.v941;

import com.swiftmq.filetransfer.protocol.MessageBasedReply;
import com.swiftmq.jms.TextMessageImpl;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

public class FileChunkReply extends MessageBasedReply
{
  public static final String CHUNKNO_PROP = "JMS_SWIFTMQ_FT_CHUNKNO";
  int chunkNo = 0;
  String link = null;

  public FileChunkReply(Message message) throws JMSException
  {
    super(message);
    chunkNo = message.getIntProperty(CHUNKNO_PROP);
    link = ((TextMessage) message).getText();
  }

  public FileChunkReply()
  {
  }

  public int getChunkNo()
  {
    return chunkNo;
  }

  public void setChunkNo(int chunkNo)
  {
    this.chunkNo = chunkNo;
  }

  public void setLink(String link)
  {
    this.link = link;
  }

  public String getLink()
  {
    return link;
  }

  public Message toMessage() throws JMSException
  {
    TextMessage message = new TextMessageImpl();
    message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILECHUNK_REP);
    message.setIntProperty(CHUNKNO_PROP, chunkNo);
    if (link != null)
      message.setText(link);
    return fillMessage(message);

  }

  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("[FileChunkReply");
    sb.append(super.toString());
    sb.append(" chunkNo='").append(chunkNo).append('\'');
    sb.append(" link='").append(link).append('\'');
    sb.append(']');
    return sb.toString();
  }
}
