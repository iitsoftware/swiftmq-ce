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

package com.swiftmq.impl.routing.single.smqpr;

import com.swiftmq.tools.requestreply.*;
import com.swiftmq.tools.concurrent.Semaphore;

public class CloseStageQueueRequest extends Request
{
  Semaphore semaphore = null;

  public CloseStageQueueRequest()
  {
    super(0,false);
  }

  public int getDumpId()
  {
    return SMQRFactory.CLOSE_STAGE_QUEUE_REQ;
  }

  public Semaphore getSemaphore()
  {
    return semaphore;
  }

  public void setSemaphore(Semaphore semaphore)
  {
    this.semaphore = semaphore;
  }

  protected Reply createReplyInstance()
  {
    return null;
  }

  public void accept(RequestVisitor visitor)
  {
  }

  public String toString()
  {
    return "[CloseStageQueueRequest, semaphore="+semaphore+"]";
  }
}
