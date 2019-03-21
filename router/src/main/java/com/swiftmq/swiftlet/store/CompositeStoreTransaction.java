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

package com.swiftmq.swiftlet.store;

import com.swiftmq.tools.concurrent.AsyncCompletionCallback;

public abstract class CompositeStoreTransaction implements StoreWriteTransaction, StoreReadTransaction
{
  public abstract void setReferencable(boolean referencable);

  public abstract boolean isReferencable();

  public abstract void setPersistentStore(PersistentStore pStore)
      throws StoreException;

  public abstract void commitTransaction()
      throws StoreException;

  public abstract void commitTransaction(AsyncCompletionCallback callback);

  public abstract void abortTransaction()
      throws StoreException;

  public abstract void abortTransaction(AsyncCompletionCallback callback);
}
