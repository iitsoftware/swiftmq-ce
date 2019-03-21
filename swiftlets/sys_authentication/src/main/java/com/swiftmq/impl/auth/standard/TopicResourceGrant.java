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

package com.swiftmq.impl.auth.standard;

public class TopicResourceGrant extends ResourceGrant
{
  boolean subscriberGranted;
  boolean publisherGranted;
  boolean createDurableGranted;

  public TopicResourceGrant(String resourceName, boolean subscriberGranted, boolean publisherGranted, boolean createDurableGranted)
  {
    super(resourceName);
    this.subscriberGranted = subscriberGranted;
    this.publisherGranted = publisherGranted;
    this.createDurableGranted = createDurableGranted;
  }

  public synchronized void setSubscriberGranted(boolean subscriberGranted)
  {
    this.subscriberGranted = subscriberGranted;
  }

  public synchronized boolean isSubscriberGranted()
  {
    return (subscriberGranted);
  }

  public synchronized void setPublisherGranted(boolean publisherGranted)
  {
    this.publisherGranted = publisherGranted;
  }

  public synchronized boolean isPublisherGranted()
  {
    return (publisherGranted);
  }

  public synchronized void setCreateDurableGranted(boolean createDurableGranted)
  {
    this.createDurableGranted = createDurableGranted;
  }

  public synchronized boolean isCreateDurableGranted()
  {
    return (createDurableGranted);
  }

  public String toString()
  {
    StringBuffer s = new StringBuffer();
    s.append("[TopicResourceGrant, resourceName=");
    s.append(resourceName);
    s.append(", subscriberGranted=");
    s.append(subscriberGranted);
    s.append(", publisherGranted=");
    s.append(publisherGranted);
    s.append(", createDurableGranted=");
    s.append(createDurableGranted);
    s.append("]");
    return s.toString();
  }
}

