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

package com.swiftmq.impl.jms.standard.v400;

import com.swiftmq.jms.smqp.v400.*;
import com.swiftmq.jms.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;
import com.swiftmq.tools.requestreply.Reply;

public class DeliveryItem extends Request
{
	MessageEntry messageEntry = null;
	Consumer consumer = null;
	AsyncMessageDeliveryRequest request = null;

  public DeliveryItem()
  {
    super(0,false);
  }

  public void accept(RequestVisitor visitor)
  {
    ((SessionVisitor)visitor).visitDeliveryItem(this);
  }

  protected Reply createReplyInstance()
  {
    return null;
  }

  public String toString()
	{
		return "[DeliveryItem, consumer="+consumer+", messageEntry="+messageEntry+", request="+request+"]";
	}
}

