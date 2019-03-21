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

package com.swiftmq.jms.v400;

import com.swiftmq.tools.requestreply.RequestRegistry;

import javax.jms.*;

public class TopicSubscriberImpl extends MessageConsumerImpl
  implements TopicSubscriber
{
  Topic topic = null;
  boolean noLocal = false;

  public TopicSubscriberImpl(boolean transacted, int acknowledgeMode,
                             int dispatchId, RequestRegistry requestRegistry,
                             Topic topic, String messageSelector,
                             SessionImpl session, boolean noLocal)
  {
    super(transacted, acknowledgeMode, dispatchId, requestRegistry,
      messageSelector, session);

    this.topic = topic;
    this.noLocal = noLocal;
  }

  public Topic getTopic()
    throws JMSException
  {
    return topic;
  }

  public boolean getNoLocal()
    throws JMSException
  {
    return noLocal;
  }
}

