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

package com.swiftmq.impl.store.standard.index;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.log.DeleteLogAction;
import com.swiftmq.impl.store.standard.log.InsertLogAction;
import com.swiftmq.impl.store.standard.log.UpdatePortionLogAction;

import java.util.*;

public abstract class Index
{
  StoreContext ctx;
  int rootPageNo = -1;
  List journal = null;
  Map pageList = null;
  int maxPage = -1;

  public Index(StoreContext ctx, int rootPageNo)
  {
    this.ctx = ctx;
    this.rootPageNo = rootPageNo;
    pageList = new HashMap();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/create");
  }

  public int getRootPageNo()
  {
    return (rootPageNo);
  }

  public void unloadPages() throws Exception
  {
    for (Iterator iter = pageList.entrySet().iterator(); iter.hasNext();)
    {
      ((IndexPage) ((Map.Entry) iter.next()).getValue()).unload();
    }
    pageList.clear();
  }

  public abstract IndexPage createIndexPage(int pageNo) throws Exception;

  IndexPage getIndexPage(int pageNo) throws Exception
  {
    Integer pn = Integer.valueOf(pageNo);
    IndexPage ip = null;
    if (pageNo != -1)
      ip = (IndexPage) pageList.get(pn);
    if (ip == null)
    {
      ip = createIndexPage(pageNo);
      pageList.put(pn, ip);
    }
    if (pageNo == -1)
      journal.add(new InsertLogAction(ip.getPage().pageNo));
    ip.setJournal(journal);
    return ip;
  }

  public void setJournal(List journal)
  {
    this.journal = journal;
  }

  public List getJournal()
  {
    // SBgen: Get variable
    return (journal);
  }

  private boolean isMatch(IndexPage current, Comparable key)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/isMatch, key=" + key + ", current=" + current);
    return current.getNumberEntries() == 0 ||
        current.getMinKey().compareTo(key) <= 0 && current.getMaxKey().compareTo(key) >= 0;
  }

  private IndexPage loadPageForKey(Comparable key) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/loadPageForKey, key=" + key + "...");
    IndexPage current = getIndexPage(rootPageNo);
    while (!isMatch(current, key))
    {
      int next = current.getNextPage();
      if (next == -1)
      {
        current = null;
        break;
      }
      current = getIndexPage(next);
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/loadPageForKey, key=" + key + ", current=" + current);
    return current;
  }

  private void rechainAfterDelete(IndexPage current) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/rechainAfterDelete...");
    if (current.getPrevPage() != -1)
    {
      IndexPage prev = getIndexPage(current.getPrevPage());
      prev.setNextPage(current.getNextPage());
    }
    if (current.getNextPage() != -1)
    {
      IndexPage next = getIndexPage(current.getNextPage());
      next.setPrevPage(current.getPrevPage());
    }
    maxPage = -1;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/rechainAfterDelete...done.");
  }

  private void moveNextToRoot(IndexPage current) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/moveNextToRoot...");
    IndexPage next = getIndexPage(current.getNextPage());
    byte[] beforeImage = new byte[current.getFirstFreePosition()];
    System.arraycopy(current.getPage().data, 0, beforeImage, 0, beforeImage.length);
    System.arraycopy(next.getPage().data, 0, current.getPage().data, 0, next.getFirstFreePosition());
    byte[] afterImage = new byte[next.getFirstFreePosition()];
    System.arraycopy(current.getPage().data, 0, afterImage, 0, afterImage.length);
    journal.add(new UpdatePortionLogAction(current.getPage().pageNo, 0, beforeImage, afterImage));
    current.setPrevPage(-1);
    current.initValues();
    journal.add(new DeleteLogAction(next.getPage().pageNo, afterImage));
    next.getPage().dirty = true;
    next.getPage().empty = true;
    if (next.getNextPage() != -1)
    {
      next = getIndexPage(next.getNextPage());
      next.setPrevPage(current.getPage().pageNo);
    }
    maxPage = -1;
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/moveNextToRoot...done, current=" + current);
  }

  int addToPage(IndexPage indexPage, IndexEntry entry) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/addToPage, entry=" + entry + "...");
    IndexPage current = indexPage;
    if (current.available() < entry.getLength())
    {
      IndexPage nextPage = getIndexPage(-1);
      current.setNextPage(nextPage.getPage().pageNo);
      nextPage.setPrevPage(current.getPage().pageNo);
      nextPage.setNextPage(-1);
      current = nextPage;
    }
    current.addEntry(entry);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/addToPage, entry=" + entry + "...done, current=" + current);
    return current.getPage().pageNo;
  }

  public List getEntries() throws Exception
  {
    List list = new ArrayList();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/getEntries...");
    IndexPage current = getIndexPage(rootPageNo);
    int next = -1;
    do
    {
      for (Iterator iter = current.iterator(); iter.hasNext();)
      {
        IndexEntry entry = (IndexEntry) iter.next();
        if (entry.isValid())
          list.add(entry);
      }
      next = current.getNextPage();
      if (next != -1)
        current = getIndexPage(next);
    } while (next != -1);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/getEntries...done.");
    return list;
  }

  public Comparable getMaxKey() throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/getMaxKey...");
    IndexPage current = getIndexPage(rootPageNo);
    while (current.getNextPage() != -1)
    {
      current = getIndexPage(current.getNextPage());
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/getMaxKey...done.");
    return current.getMaxKey();
  }

  /**
   * @param key
   */
  public IndexEntry find(Comparable key) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/find, key=" + key);
    IndexPage current = loadPageForKey(key);
    if (current == null)
      return null;
    // Just to avoid iterating
    if (current.getNumberValidEntries() == 0 || current.getMaxKey().compareTo(key) < 0)
      return null;
    for (Iterator iter = current.iterator(); iter.hasNext();)
    {
      IndexEntry entry = (IndexEntry) iter.next();
      if (entry.isValid())
      {
        int cmp = entry.getKey().compareTo(key);
        if (cmp == 0)
          return entry;
        else if (cmp > 0)
          return null;
      }
    }
    return null;
  }

  /**
   * @param key
   */
  public void replace(Comparable key, IndexEntry newEntry) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/update, key=" + key);
    IndexPage current = loadPageForKey(key);
    for (Iterator iter = current.iterator(); iter.hasNext();)
    {
      IndexEntry entry = (IndexEntry) iter.next();
      if (entry.isValid())
      {
        int cmp = entry.getKey().compareTo(key);
        if (cmp == 0)
        {
          current.replace(newEntry);
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/update, key=" + key + ", replaced with " + newEntry);
          return;
        }
      }
    }
  }

  /**
   * @param entry
   */
  protected void add(IndexEntry entry) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/add, entry=" + entry + "...");
    if (journal == null)
      throw new NullPointerException("journal is null!");
    IndexPage current = null;
    if (maxPage != -1)
      current = getIndexPage(maxPage);
    else
      current = getIndexPage(rootPageNo);
    while (!(current.getNextPage() == -1 || current.getNumberValidEntries() == 0 || current.getMaxKey().compareTo(entry.getKey()) > 0))
    {
      current = getIndexPage(current.getNextPage());
    }
    maxPage = addToPage(current, entry);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/add, entry=" + entry + "...done.");
  }

  /**
   * @param key
   */
  public void remove(Comparable key) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/remove, key=" + key + "...");
    if (journal == null)
      throw new NullPointerException("journal is null!");
    IndexPage current = loadPageForKey(key);
    if (current == null)
      throw new Exception("remove(" + key + "): page not found!");
    boolean found = false;
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/remove, before=" + current.getContent());
    for (Iterator iter = current.iterator(); iter.hasNext();)
    {
      IndexEntry entry = (IndexEntry) iter.next();
      if (entry.isValid())
      {
        if (entry.getKey().equals(key))
        {
          iter.remove();
          if (current.getNumberValidEntries() == 0)
          {
            if (current.getPage().pageNo == rootPageNo)
            {
              current.getPage().dirty = true;
              current.getPage().empty = false; // never delete the root
              if (current.getNextPage() != -1)
                moveNextToRoot(current);
            } else
            {
              byte[] bi = new byte[current.getFirstFreePosition()];
              System.arraycopy(current.getPage().data, 0, bi, 0, bi.length);
              journal.add(new DeleteLogAction(current.getPage().pageNo, bi));
              current.getPage().dirty = true;
              current.getPage().empty = true;
              rechainAfterDelete(current);
            }
          }
          found = true;
          break;
        }
      }
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/remove, found=" + found + ", after=" + current.getContent());
    if (!found)
      throw new Exception("remove(" + key + "): key not found in page!");
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/remove, key=" + key + "...done.");
  }

  public String toString()
  {
    return "rootPageNo=" + rootPageNo;
  }
}

