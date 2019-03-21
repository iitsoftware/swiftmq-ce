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

package com.swiftmq.impl.topic.standard.announce.v400;

import com.swiftmq.impl.topic.standard.TopicManagerImpl;
import com.swiftmq.impl.topic.standard.announce.TopicInfo;
import com.swiftmq.tools.dump.Dumpable;

import java.io.*;

public class TopicInfoImpl implements TopicInfo, Dumpable
{
  String destination;
  String routerName;
  String topicName;
  String[] tokenizedPredicate;
  int numberSubscriptions;
  boolean creationInfo = false;

  public TopicInfoImpl(String routerName, String topicName, String[] tokenizedPredicate, int numberSubscriptions)
  {
    this.routerName = routerName;
    this.topicName = topicName;
    this.tokenizedPredicate = tokenizedPredicate;
    this.numberSubscriptions = numberSubscriptions;
  }

  public TopicInfoImpl(String destination, String routerName, String topicName, String[] tokenizedPredicate, int numberSubscriptions)
  {
    this.destination = destination;
    this.routerName = routerName;
    this.topicName = topicName;
    this.tokenizedPredicate = tokenizedPredicate;
    this.numberSubscriptions = numberSubscriptions;
  }

  public TopicInfoImpl(String destination, String routerName, String topicName, String[] tokenizedPredicate, boolean creationInfo)
  {
    this.destination = destination;
    this.routerName = routerName;
    this.topicName = topicName;
    this.tokenizedPredicate = tokenizedPredicate;
    this.numberSubscriptions = 0;
    this.creationInfo = creationInfo;
  }

  public TopicInfoImpl()
  {
  }

  public int getDumpId()
  {
    return 0;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    if (destination != null)
    {
      out.writeByte(1);
      out.writeUTF(destination);
    } else
      out.writeByte(0);
    if (routerName != null)
    {
      out.writeByte(1);
      out.writeUTF(routerName);
    } else
      out.writeByte(0);
    if (topicName != null)
    {
      out.writeByte(1);
      out.writeUTF(topicName);
    } else
      out.writeByte(0);
    if (tokenizedPredicate != null)
    {
      out.writeByte(1);
      out.writeInt(tokenizedPredicate.length);
      for (int i=0;i<tokenizedPredicate.length;i++)
        out.writeUTF(tokenizedPredicate[i]);
    } else
      out.writeByte(0);
    out.writeInt(numberSubscriptions);
    out.writeBoolean(creationInfo);
  }

  public void readContent(DataInput in) throws IOException
  {
    byte set = in.readByte();
    if (set == 1)
      destination = in.readUTF();
    set = in.readByte();
    if (set == 1)
      routerName = in.readUTF();
    set = in.readByte();
    if (set == 1)
      topicName = in.readUTF();
    set = in.readByte();
    if (set == 1)
    {
      tokenizedPredicate = new String[in.readInt()];
      for (int i=0;i<tokenizedPredicate.length;i++)
        tokenizedPredicate[i] = in.readUTF();
    }
    numberSubscriptions = in.readInt();
    creationInfo = in.readBoolean();
  }

  public String getDestination()
  {
    return (destination);
  }

  public String getRouterName()
  {
    return (routerName);
  }

  public boolean isCreationInfo()
  {
    return creationInfo;
  }

  public String getTopicName()
  {
    return (topicName);
  }

  public String[] getTokenizedPredicate()
  {
    return (tokenizedPredicate);
  }

  public int getNumberSubscriptions()
  {
    return (numberSubscriptions);
  }

  private static String concatName(String[] tokenizedName)
  {
    if (tokenizedName == null)
      return null;
    StringBuffer s = new StringBuffer();
    for (int i = 0; i < tokenizedName.length; i++)
    {
      if (i != 0)
        s.append(TopicManagerImpl.TOPIC_DELIMITER);
      s.append(tokenizedName[i]);
    }
    return s.toString();
  }

  public String toString()
  {
    return "[TopicInfoImpl (v400)" + (creationInfo?" (CREATIONINFO)":"") +
      ", destination=" + destination +
      ", routerName=" + routerName +
      ", topicName=" + topicName +
      ", tokenizedPredicate=" + concatName(tokenizedPredicate) +
      ", numberSubscriptions=" + numberSubscriptions + "]";
  }
}

