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

package com.swiftmq.impl.queue.standard.composite;

import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.AbstractQueue;

public class CompositeTransactionIdEntry
{
  boolean isQueue = true;
  AbstractQueue queue = null;
  Object txId = null;
  MessageSelector selector = null;
  boolean generateNewMessageId = false;
  boolean changeDestination = false;
  boolean isDefaultBinding = false;
  String originalName = null;

  public CompositeTransactionIdEntry(boolean isQueue, AbstractQueue queue, Object txId, MessageSelector selector, boolean generateNewMessageId, boolean changeDestination, boolean isDefaultBinding, String originalName)
  {
    this.isQueue = isQueue;
    this.queue = queue;
    this.txId = txId;
    this.selector = selector;
    this.generateNewMessageId = generateNewMessageId;
    this.changeDestination = changeDestination;
    this.isDefaultBinding = isDefaultBinding;
    this.originalName = originalName;
  }
}
