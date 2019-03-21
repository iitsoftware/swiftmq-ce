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

package com.swiftmq.impl.topic.standard.announce;

import com.swiftmq.tools.versioning.*;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import java.io.IOException;

public class TopicInfoFactory extends VersionedFactory
{
  static final int[] VERSIONS = {400};
  DataByteArrayInputStream dis = new DataByteArrayInputStream();

  public static int[] getSupportedVersions()
  {
    return VERSIONS;
  }

  public static VersionedDumpable createTopicInfo(String destination, String routerName, String topicName, String[] tokenizedName, boolean createInfo)
  {
    return new VersionedDumpable(400,new com.swiftmq.impl.topic.standard.announce.v400.TopicInfoImpl(destination,routerName,topicName,tokenizedName,createInfo));
  }

  public static VersionedDumpable createTopicInfo(String destination, String routerName, String topicName, String[] tokenizedName, int subCnt)
  {
    return new VersionedDumpable(400,new com.swiftmq.impl.topic.standard.announce.v400.TopicInfoImpl(destination,routerName,topicName,tokenizedName,subCnt));
  }

  public static TopicInfo createTopicInfo(String routerName, String topicName, String[] tokenizedName, int subCnt)
  {
    return new  com.swiftmq.impl.topic.standard.announce.v400.TopicInfoImpl(routerName,topicName,tokenizedName,subCnt);
  }

  protected Dumpable createDumpable(Versioned versioned) throws VersionedException
  {
    int version = versioned.getVersion();
    Dumpable d = null;
    switch (version)
    {
      case 400:
        d = new com.swiftmq.impl.topic.standard.announce.v400.TopicInfoImpl();
        break;
    }
    if (d != null)
    {
      try
      {
        dis.reset();
        dis.setBuffer(versioned.getPayload(),0,versioned.getLength());
        dis.readInt(); // dumpid
        d.readContent(dis);
      } catch (IOException e)
      {
        throw new VersionedException(e.toString());
      }
    }
    return d;
  }
}
