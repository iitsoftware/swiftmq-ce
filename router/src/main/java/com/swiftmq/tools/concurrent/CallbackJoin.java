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

package com.swiftmq.tools.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class CallbackJoin
{
  AtomicInteger numberCallbacks = new AtomicInteger();
  volatile boolean blocked = true;
  AsyncCompletionCallback next;
  protected volatile boolean finalSuccess = true;
  protected volatile Object finalResult = null;
  protected volatile Exception finalException = null;

  protected CallbackJoin(AsyncCompletionCallback next)
  {
    this.next = next;
  }

  protected CallbackJoin()
  {
  }

  private synchronized AsyncCompletionCallback getNext()
  {
    if (blocked || next == null)
      return null;
    AsyncCompletionCallback myNext = next;
    next = null;
    return myNext;
  }

  private void checkNext()
  {
    AsyncCompletionCallback theNext = getNext();
    if (theNext != null)
    {
      theNext.setException(finalException);
      theNext.setResult(finalResult);
      theNext.notifyCallbackStack(finalSuccess);
    }
  }

  public void incNumberCallbacks()
  {
    numberCallbacks.incrementAndGet();
  }

  public void setNumberCallbacks(int numberCallbacks)
  {
    this.numberCallbacks.set(numberCallbacks);
  }

  public void setBlocked(boolean blocked)
  {
    this.blocked = blocked;
    if (numberCallbacks.get() == 0)
      checkNext();
  }

  public void done(AsyncCompletionCallback callback, boolean success)
  {
    int n = numberCallbacks.decrementAndGet();
    callbackDone(callback, success, n == 0);
    if (n == 0)
      checkNext();
  }

  protected abstract void callbackDone(AsyncCompletionCallback callback, boolean success, boolean last);
}
