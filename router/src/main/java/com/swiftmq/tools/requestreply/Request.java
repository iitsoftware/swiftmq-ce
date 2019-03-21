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

package com.swiftmq.tools.requestreply;

import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.dump.Dumpable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public abstract class Request implements Dumpable, Serializable
{
  public final static int NO_TIMEOUT = -1;
  public volatile Semaphore _sem = null;
  int dispatchId = -1;
  int requestNumber = 0;
  int correlationId = 0;
  transient volatile Reply reply = null;
  long timeout = NO_TIMEOUT;
  boolean replyRequired = true;
  transient volatile ReplyHandler replyHandler;
  transient volatile RequestRetryValidator validator = null;
  transient volatile boolean doRetry = false;
  transient volatile boolean wasRetry = false;
  transient volatile boolean cancelledByValidator = false;
  transient int connectionId = -1;

  public Request(int dispatchId, boolean replyRequired)
  {
    this.dispatchId = dispatchId;
    this.replyRequired = replyRequired;
  }

  public Request(int dispatchId, boolean replyRequired, RequestRetryValidator validator)
  {
    this.dispatchId = dispatchId;
    this.replyRequired = replyRequired;
    this.validator = validator;
  }

  public boolean isDoRetry()
  {
    return doRetry;
  }

  public void setDoRetry(boolean doRetry)
  {
    this.doRetry = doRetry;
    if (doRetry)
      wasRetry = true;
  }

  public boolean isWasRetry()
  {
    return wasRetry;
  }

  public RequestRetryValidator getValidator()
  {
    return validator;
  }

  public void setValidator(RequestRetryValidator validator)
  {
    this.validator = validator;
  }

  public boolean isCancelledByValidator()
  {
    return cancelledByValidator;
  }

  public void setCancelledByValidator(boolean cancelledByValidator)
  {
    this.cancelledByValidator = cancelledByValidator;
  }

  public int getConnectionId()
  {
    return connectionId;
  }

  public void setConnectionId(int connectionId)
  {
    this.connectionId = connectionId;
  }

  public int getDumpId()
  {
    return (0);
  }

  public void writeContent(DataOutput out)
      throws IOException
  {
    out.writeInt(dispatchId);
    out.writeInt(requestNumber);
    out.writeInt(correlationId);
    out.writeLong(timeout);
    out.writeBoolean(replyRequired);
  }

  public void readContent(DataInput in)
      throws IOException
  {
    dispatchId = in.readInt();
    requestNumber = in.readInt();
    correlationId = in.readInt();
    timeout = in.readLong();
    replyRequired = in.readBoolean();
  }

  public int getDispatchId()
  {
    return (dispatchId);
  }

  public void setDispatchId(int dispatchId)
  {
    this.dispatchId = dispatchId;
  }

  public void setReplyRequired(boolean replyRequired)
  {
    this.replyRequired = replyRequired;
  }

  public boolean isReplyRequired()
  {
    return (replyRequired);
  }

  void setTimeout(long timeout)
  {
    this.timeout = timeout;
  }

  long getTimeout()
  {
    return (timeout);
  }

  void setRequestNumber(int requestNumber)
  {
    this.requestNumber = requestNumber;
  }

  int getRequestNumber()
  {
    return (requestNumber);
  }

  void setReply(Reply reply)
  {
    this.reply = reply;
  }

  Reply getReply()
  {
    return (reply);
  }

  public int getCorrelationId()
  {
    return correlationId;
  }

  public void setCorrelationId(int correlationId)
  {
    this.correlationId = correlationId;
  }

  void setReplyHandler(ReplyHandler replyHandler)
  {
    this.replyHandler = replyHandler;
  }

  protected abstract Reply createReplyInstance();

  public Reply createReply()
  {
    Reply reply = createReplyInstance();
    reply.setRequestNumber(requestNumber);
    reply.setReplyHandler(replyHandler);
    return reply;
  }

  public abstract void accept(RequestVisitor visitor);

  public String toString()
  {
    return "[Request, dispatchId=" + dispatchId + " requestNumber=" + requestNumber + " correlationId=" + correlationId +
        " timeout=" + timeout + " replyRequired=" + replyRequired + " reply=" + reply + "]";
  }
}

