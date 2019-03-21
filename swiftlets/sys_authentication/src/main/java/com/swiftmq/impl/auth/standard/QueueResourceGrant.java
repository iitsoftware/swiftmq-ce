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

public class QueueResourceGrant extends ResourceGrant
{
  boolean receiverGranted;
  boolean senderGranted;
  boolean browserGranted;

  public QueueResourceGrant(String resourceName, boolean receiverGranted, boolean senderGranted, boolean browserGranted)
  {
    super(resourceName);
    this.receiverGranted = receiverGranted;
    this.senderGranted = senderGranted;
    this.browserGranted = browserGranted;
  }

  public synchronized void setReceiverGranted(boolean receiverGranted)
  {
    this.receiverGranted = receiverGranted;
  }

  public synchronized boolean isReceiverGranted()
  {
    return (receiverGranted);
  }

  public synchronized void setSenderGranted(boolean senderGranted)
  {
    this.senderGranted = senderGranted;
  }

  public synchronized boolean isSenderGranted()
  {
    return (senderGranted);
  }

  public synchronized void setBrowserGranted(boolean browserGranted)
  {
    this.browserGranted = browserGranted;
  }

  public synchronized boolean isBrowserGranted()
  {
    return (browserGranted);
  }

  public String toString()
  {
    StringBuffer s = new StringBuffer();
    s.append("[QueueResourceGrant, resourceName=");
    s.append(resourceName);
    s.append(", receiverGranted=");
    s.append(receiverGranted);
    s.append(", senderGranted=");
    s.append(senderGranted);
    s.append(", browserGranted=");
    s.append(browserGranted);
    s.append("]");
    return s.toString();
  }
}

