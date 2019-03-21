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

import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.mgmt.PropertyChangeListener;
import com.swiftmq.tools.file.NumberGenerationProvider;
import com.swiftmq.tools.file.RollingFileWriter;
import com.swiftmq.tools.file.RolloverSizeProvider;
import com.swiftmq.tools.sql.LikeComparator;
import com.swiftmq.util.SwiftUtilities;

import java.io.PrintWriter;
import java.util.Hashtable;

class TraceDestination
{
  static final String VAL_CONSOLE = "console";

  TraceSwiftletImpl traceSwiftlet = null;
  Entity entity = null;
  volatile boolean enabled;
  volatile String predicate;
  volatile PrintWriter writer;
  Hashtable writerCache = null;
  NumberGenerationProvider numberGenerationProvider = null;

  TraceDestination(final TraceSwiftletImpl traceSwiftlet, Entity entity, Hashtable writerCache) throws Exception
  {
    this.traceSwiftlet = traceSwiftlet;
    this.entity = entity;
    this.writerCache = writerCache;
    numberGenerationProvider = new NumberGenerationProvider() {
      @Override
      public int getNumberGenerations() {
        return traceSwiftlet.getNumberFileGenerations();
      }
    };

    Property prop = entity.getProperty("enabled");
    enabled = ((Boolean) prop.getValue()).booleanValue();
    prop.setPropertyChangeListener(new PropertyChangeListener()
    {
      public void propertyChanged(Property prop, Object oldValue, Object newValue) throws PropertyChangeException
      {
        enabled = ((Boolean) newValue).booleanValue();
      }
    });
    prop = entity.getProperty("value");
    predicate = (String) prop.getValue();
    prop.setPropertyChangeListener(new PropertyChangeListener()
    {
      public void propertyChanged(Property prop, Object oldValue, Object newValue) throws PropertyChangeException
      {
        predicate = (String) newValue;
      }
    });
    prop = entity.getProperty("filename");
    writer = getWriter((String) prop.getValue());
    prop.setPropertyChangeListener(new PropertyChangeListener()
    {
      public void propertyChanged(Property prop, Object oldValue, Object newValue) throws PropertyChangeException
      {
        try
        {
          writer = getWriter((String) newValue);
        } catch (Exception e)
        {
          throw new PropertyChangeException(e.toString());
        }
      }
    });
  }

  private PrintWriter getWriter(String filename) throws Exception
  {
    PrintWriter w = (PrintWriter) writerCache.get(filename);
    if (w == null)
    {
      if (filename.equals(VAL_CONSOLE))
      {
        w = new PrintWriter(System.out);
      } else
      {
        filename = SwiftUtilities.addWorkingDir(filename);
        SwiftUtilities.createDirectoryOfFile(filename);
        w = new PrintWriter(new RollingFileWriter(filename, new RolloverSizeProvider()
        {
          public long getRollOverSize()
          {
            return traceSwiftlet.getMaxFileSize();
          }
        }, numberGenerationProvider));
      }
      writerCache.put(filename, w);
    }
    return w;
  }

  boolean isEnabled()
  {
    return enabled;
  }

  void trace(String subEntity, String message)
  {
    if (!enabled)
      return;
    PrintWriter w = writer;
    if (w != null && equals(subEntity))
    {
      w.println(message);
      w.flush();
    }
  }

  boolean equals(String that)
  {
    return that != null && LikeComparator.compare(that, predicate, '\\');
  }
}
