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

package com.swiftmq.jms.smqp.v510;

/** SMQP-Protocol Version 510, Class: CreateSubscriberRequest
 *  Automatically generated, don't change!
 *  Generation Date: Fri Aug 13 16:00:44 CEST 2004
 *  (c) 2004, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.*;
import com.swiftmq.jms.v510.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.*;
import java.io.*;
import java.util.*;
import javax.jms.*;

public class CreateSubscriberRequest extends Request
{
  private TopicImpl topic;
  private String messageSelector;
  private boolean noLocal;
  private boolean autoCommit;

  public CreateSubscriberRequest()
  {
    super(0,true);
  }

  public CreateSubscriberRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public CreateSubscriberRequest(int dispatchId, TopicImpl topic, String messageSelector, boolean noLocal, boolean autoCommit)
  {
    super(dispatchId,true);
    this.topic = topic;
    this.messageSelector = messageSelector;
    this.noLocal = noLocal;
    this.autoCommit = autoCommit;
  }
  
  public void setTopic(TopicImpl topic)
  {
    this.topic = topic;
  }

  public TopicImpl getTopic()
  {
    return topic;
  }
  
  public void setMessageSelector(String messageSelector)
  {
    this.messageSelector = messageSelector;
  }

  public String getMessageSelector()
  {
    return messageSelector;
  }
  
  public void setNoLocal(boolean noLocal)
  {
    this.noLocal = noLocal;
  }

  public boolean isNoLocal()
  {
    return noLocal;
  }
  
  public void setAutoCommit(boolean autoCommit)
  {
    this.autoCommit = autoCommit;
  }

  public boolean isAutoCommit()
  {
    return autoCommit;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_CREATESUBSCRIBER_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(topic,out);
    if (messageSelector != null)
    {
      out.writeBoolean(true);
      SMQPUtil.write(messageSelector,out);
    } else
      out.writeBoolean(false);
    SMQPUtil.write(noLocal,out);
    SMQPUtil.write(autoCommit,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    topic = SMQPUtil.read(topic,in);
    boolean messageSelector_set = in.readBoolean();
    if (messageSelector_set)
      messageSelector = SMQPUtil.read(messageSelector,in);
    noLocal = SMQPUtil.read(noLocal,in);
    autoCommit = SMQPUtil.read(autoCommit,in);
  }

  protected Reply createReplyInstance()
  {
    return new CreateSubscriberReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[CreateSubscriberRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("topic=");
    _b.append(topic);
    _b.append(", ");
    _b.append("messageSelector=");
    _b.append(messageSelector);
    _b.append(", ");
    _b.append("noLocal=");
    _b.append(noLocal);
    _b.append(", ");
    _b.append("autoCommit=");
    _b.append(autoCommit);
    _b.append("]");
    return _b.toString();
  }
}
