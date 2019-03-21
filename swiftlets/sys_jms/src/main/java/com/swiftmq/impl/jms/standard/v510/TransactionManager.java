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

package com.swiftmq.impl.jms.standard.v510;

import com.swiftmq.swiftlet.queue.QueueTransaction;

import java.util.*;

public class TransactionManager
{
  SessionContext ctx = null;
  List transactionFactories = new ArrayList();
  List transactions = new ArrayList();

  TransactionManager(SessionContext ctx)
  {
    this.ctx = ctx;
  }

  synchronized void addTransactionFactory(TransactionFactory transactionFactory) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/addTransactionFactory, transactionFactory=" + transactionFactory);
    transactionFactories.add(transactionFactory);
    transactions.add(transactionFactory.createTransaction());
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/addTransactionFactory done, transactionFactory=" + transactionFactory);
  }

  synchronized void removeTransactionFactory(TransactionFactory transactionFactory)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/removeTransactionFactory, transactionFactory=" + transactionFactory);
    transactionFactories.remove(transactionFactory);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/removeTransactionFactory done, transactionFactory=" + transactionFactory);
  }

  synchronized void startTransactions() throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/startTransactions");
    transactions.clear();
    for (Iterator iter = transactionFactories.iterator(); iter.hasNext();)
    {
      TransactionFactory f = (TransactionFactory) iter.next();
      if (!f.isMarkedForClose())
        transactions.add(f.createTransaction());
      else
        iter.remove();
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/startTransactions done");
  }

  synchronized void commit() throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/commit");
    for (Iterator iter = transactions.iterator(); iter.hasNext();)
    {
      QueueTransaction t = (QueueTransaction) iter.next();
      t.commit();
    }
    startTransactions();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/commit done");
  }

  void rollback() throws Exception
  {
    rollback(true);
  }

  synchronized void rollback(boolean start) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/rollback");
    for (Iterator iter = transactions.iterator(); iter.hasNext();)
    {
      QueueTransaction t = (QueueTransaction) iter.next();
      t.rollback();
    }
    if (start)
      startTransactions();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/rollback done");
  }

  synchronized void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/close");
    try
    {
      rollback(false);
    } catch (Exception ignored)
    {
    }
    transactions.clear();
    transactionFactories.clear();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/close done");
  }

  public String toString()
  {
    return "TransactionManager";
  }
}

