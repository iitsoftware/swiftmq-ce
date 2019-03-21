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

package com.swiftmq.impl.trace.standard;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.trace.*;

import java.io.PrintWriter;
import java.util.*;

public class TraceSwiftletImpl extends TraceSwiftlet
{
  Configuration config = null;
  Hashtable files = null;
  Hashtable spaces = null;
  volatile long maxFileSize = -1;

  public long getMaxFileSize()
  {
    return maxFileSize;
  }

  public int getNumberFileGenerations()
  {
    return ((Integer)config.getProperty("number-old-tracefile-generations").getValue()).intValue();
  }
  /**
   *
   * Abstract factory method to create a trace space. In any case it has to
   * return a valid trace space object
   * @param spaceName space name
   * @return a valid trace space object
   */
  protected TraceSpace createTraceSpace(String spaceName)
  {
    TraceSpace space = (TraceSpace) spaces.get(spaceName);
    if (space != null)
      return space;
    return new TraceSpaceImpl(spaceName, false);
  }

  private TraceDestination createPredicate(Entity predicateEntity) throws Exception
  {
    return new TraceDestination(this, predicateEntity, files);
  }

  private void createPredicates(EntityList predicateList) throws Exception
  {
    Map m = predicateList.getEntities();
    for (Iterator iter = m.entrySet().iterator(); iter.hasNext();)
    {
      Entity predEntity = (Entity) ((Map.Entry) iter.next()).getValue();
      predEntity.setUserObject(createPredicate(predEntity));
    }
    predicateList.setEntityAddListener(new EntityChangeAdapter(null)
    {
      public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException
      {
        try
        {
          newEntity.setUserObject(createPredicate(newEntity));
        } catch (Exception e)
        {
          throw new EntityAddException(e.toString());
        }
      }
    });
  }

  private void createSpace(Entity spaceEntity) throws Exception
  {
    EntityList predicateList = (EntityList) spaceEntity.getEntity("predicates");
    createPredicates(predicateList);
    TraceSpaceImpl space = new TraceSpaceImpl(spaceEntity, predicateList);
    spaces.put(spaceEntity.getName(), space);
  }

  private void createSpaces(EntityList spaceList) throws Exception
  {
    Map m = spaceList.getEntities();
    if (m.size() > 0)
    {
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext();)
      {
        createSpace((Entity) ((Map.Entry) iter.next()).getValue());
      }
    }
    spaceList.setEntityAddListener(new EntityChangeAdapter(null)
    {
      public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException
      {
        try
        {
          createSpace(newEntity);
        } catch (Exception e)
        {
          throw new EntityAddException(e.toString());
        }
      }
    });
  }

  protected void startup(Configuration config) throws SwiftletException
  {
    this.config = config;
    files = new Hashtable();
    spaces = new Hashtable();
    Property prop = config.getProperty("max-file-size");
    int mfs = ((Integer) prop.getValue()).intValue();
    if (mfs <= 0)
      maxFileSize = -1;
    else
      maxFileSize = mfs * 1024;
    prop.setPropertyChangeListener(new PropertyChangeAdapter(null)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
        throws PropertyChangeException
      {
        int n = ((Integer) newValue).intValue();
        if (n <= 0)
          maxFileSize = -1;
        else
          maxFileSize = n * 1024;
      }
    });
    EntityList spaceList = (EntityList) config.getEntity("spaces");
    try
    {
      createSpaces(spaceList);
    } catch (Exception e)
    {
      throw new SwiftletException(e.getMessage());
    }
  }

  protected void shutdown() throws SwiftletException
  {
    super.shutdown();
    for (Iterator iter = files.entrySet().iterator(); iter.hasNext();)
    {
      Map.Entry entry = (Map.Entry) iter.next();
      String name = (String) entry.getKey();
      PrintWriter writer = (PrintWriter) entry.getValue();
      try
      {
        if (!name.equals(TraceDestination.VAL_CONSOLE))
          writer.close();
      } catch (Exception ignored)
      {
      }
    }
    files.clear();
    spaces.clear();
  }

}
