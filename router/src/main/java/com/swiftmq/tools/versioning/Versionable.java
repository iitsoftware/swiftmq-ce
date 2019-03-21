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

package com.swiftmq.tools.versioning;

import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.DestinationFactory;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.BytesMessage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Versionable implements Dumpable, Serializable
{
  List versionedList = new ArrayList();
  VEntry selected = null;
  ClassLoader classLoader = null;

  public void setClassLoader(ClassLoader classLoader)
  {
    this.classLoader = classLoader;
  }

  public static Versionable toVersionable(BytesMessage msg) throws Exception
  {
    byte[] b = new byte[(int) ((BytesMessageImpl) msg)._getBodyLength()];
    msg.readBytes(b);
    DataByteArrayInputStream dis = new DataByteArrayInputStream(b);
    Versionable v = new Versionable();
    v.readContent(dis);
    return v;
  }

  public void transferToMessage(BytesMessage msg) throws Exception
  {
    DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
    writeContent(dos);
    msg.writeBytes(dos.getBuffer(), 0, dos.getCount());
  }

  public void addVersioned(int version, Versioned versioned, String factoryClass)
  {
    versionedList.add(new VEntry(version, versioned, factoryClass));
  }

  private String print(int[] a)
  {
    StringBuffer b = new StringBuffer("[");
    for (int i = 0; i < a.length; i++)
    {
      if (i > 0)
        b.append(", ");
      b.append(a[i]);
    }
    b.append("]");
    return b.toString();
  }

  public int selectVersions(int[] versions) throws VersionedException
  {
    for (int i = 0; i < versionedList.size(); i++)
    {
      VEntry entry = (VEntry) versionedList.get(i);
      for (int j = versions.length - 1; j >= 0; j--)
      {
        if (entry.versioned.getVersion() == -1 || entry.versioned.getVersion() == versions[j])
        {
          selected = entry;
          break;
        }
      }
    }
    if (selected == null)
      throw new VersionedException("Unable to find a versioned object matching this versions: " + print(versions));
    return selected.version;
  }

  private Object create(VEntry entry) throws Exception
  {
    DataByteArrayInputStream dis = new DataByteArrayInputStream();
    dis.setBuffer(entry.versioned.getPayload(), 0, entry.versioned.getLength());
    Object obj = null;
    if (entry.version == -1)
    {
      obj = DestinationFactory.createDestination(dis);
    } else
    {
      DumpableFactory factory = null;
      if (classLoader == null)
        factory = (DumpableFactory) Class.forName(entry.factoryClass).newInstance();
      else
        factory = (DumpableFactory) classLoader.loadClass(entry.factoryClass).newInstance();
      Dumpable d = factory.createDumpable(dis.readInt());
      d.readContent(dis);
      obj = d;
    }
    return obj;
  }

  public Object createVersionedObject() throws Exception
  {
    if (selected == null)
      throw new Exception("No version selected!");
    return create(selected);
  }

  public Object createCurrentVersionObject() throws Exception
  {
    return create((VEntry) versionedList.get(versionedList.size() - 1));
  }

  public int getDumpId()
  {
    return 0;
  }

  public void writeContent(DataOutput out)
      throws IOException
  {
    out.writeInt(versionedList.size());
    for (int i = 0; i < versionedList.size(); i++)
    {
      VEntry entry = (VEntry) versionedList.get(i);
      out.writeInt(entry.version);
      entry.versioned.writeContent(out);
      out.writeUTF(entry.factoryClass);
    }
  }

  public void readContent(DataInput in)
      throws IOException
  {
    versionedList = new ArrayList();
    int size = in.readInt();
    for (int i = 0; i < size; i++)
    {
      int version = in.readInt();
      Versioned versioned = new Versioned();
      versioned.readContent(in);
      String factoryClass = in.readUTF();
      versionedList.add(new VEntry(version, versioned, factoryClass));
    }
  }

  public String toString()
  {
    return "[Versionable, versionedList=" + versionedList + "]";
  }

  private class VEntry
  {
    int version = 0;
    Versioned versioned = null;
    String factoryClass = null;

    public VEntry(int version, Versioned versioned, String factoryClass)
    {
      this.version = version;
      this.versioned = versioned;
      this.factoryClass = factoryClass;
    }

    public String toString()
    {
      return "[VEntry, version=" + version + ", versioned=" + versioned + ", factoryClass=" + factoryClass + "]";
    }
  }
}
