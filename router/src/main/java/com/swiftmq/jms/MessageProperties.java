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

package com.swiftmq.jms;

import com.swiftmq.jms.primitives.*;
import com.swiftmq.tools.dump.Dumpable;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class MessageProperties implements Enumeration
{
  Map map = null;
  static ThreadLocal iterHolder = new ThreadLocal();

  public void writeContent(DataOutput out)
      throws IOException
  {
    out.writeInt(map.size());
    for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); )
    {
      Map.Entry entry = (Map.Entry) iter.next();
      out.writeUTF((String) entry.getKey());
      Dumpable d = (Dumpable) entry.getValue();
      out.writeInt(d.getDumpId());
      d.writeContent(out);
    }
  }

  public void readContent(DataInput in)
      throws IOException
  {
    map = new TreeMap();
    int size = in.readInt();
    for (int i = 0; i < size; i++)
    {
      String name = in.readUTF();
      Dumpable d = createDumpable(in.readInt());
      d.readContent(in);
      map.put(name, d);
    }
  }

  private Dumpable createDumpable(int dumpId)
  {
    return (Dumpable) PrimitiveFactory.createInstance(dumpId);
  }

  private synchronized void checkMap()
  {
    if (map == null)
      map = new TreeMap();
  }

  void setBoolean(String name, boolean value) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _Boolean(value));
  }

  void setShort(String name, short value) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _Short(value));
  }

  void setInt(String name, int value) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _Int(value));
  }

  void setLong(String name, long value) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _Long(value));
  }

  void setDouble(String name, double value) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _Double(value));
  }

  void setFloat(String name, float value) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _Float(value));
  }

  void setChar(String name, char value) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _Char(value));
  }

  void setByte(String name, byte value) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _Byte(value));
  }

  void setBytes(String name, byte[] value) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _Bytes(value));
  }

  void setBytes(String name, byte[] value, int offset, int length) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _Bytes(value, offset, length));
  }

  void setString(String name, String value) throws JMSException
  {
    checkMap();
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    map.put(name, new _String(value));
  }

  void setObject(String name, Object value, boolean withBytes) throws JMSException
  {
    // JMS 1.1
    if (name == null || name.length() == 0)
      throw new IllegalArgumentException("Name is null");
    if (value instanceof Boolean)
      setBoolean(name, ((Boolean) value).booleanValue());
    else if (value instanceof Byte)
      setByte(name, ((Byte) value).byteValue());
    else if (value instanceof Short)
      setShort(name, ((Short) value).shortValue());
    else if (value instanceof Integer)
      setInt(name, ((Integer) value).intValue());
    else if (value instanceof Character)
      setChar(name, ((Character) value).charValue());
    else if (value instanceof Long)
      setLong(name, ((Long) value).longValue());
    else if (value instanceof Float)
      setFloat(name, ((Float) value).floatValue());
    else if (value instanceof Double)
      setDouble(name, ((Double) value).doubleValue());
    else if (value instanceof String)
      setString(name, (String) value);
    else if (value instanceof byte[] && withBytes)
      setBytes(name, (byte[]) value);
    else
      throw new MessageFormatException("Invalid object format. Only primitives are supported.");

  }

  private Object getValue(String name)
  {
    checkMap();
    Primitive primitive = (Primitive) map.get(name);
    if (primitive != null)
      return primitive.getObject();
    return null;
  }

  boolean getBoolean(String name) throws JMSException
  {
    Object obj = getValue(name);

    if (obj == null)
      return Boolean.valueOf((String) obj).booleanValue();
    if (obj instanceof Boolean)
      return ((Boolean) obj).booleanValue();
    if (obj instanceof String)
      return Boolean.valueOf((String) obj).booleanValue();

    throw new MessageFormatException("can't convert message value to boolean");
  }

  byte getByte(String name) throws JMSException
  {
    Object obj = getValue(name);

    if (obj == null)
      return Byte.valueOf((String) obj).byteValue();
    if (obj instanceof Byte)
      return ((Byte) obj).byteValue();
    if (obj instanceof String)
      return Byte.valueOf((String) obj).byteValue();
    throw new MessageFormatException("can't convert message value to byte");
  }

  short getShort(String name) throws JMSException
  {
    Object obj = getValue(name);

    if (obj == null)
      return Short.valueOf((String) obj).shortValue();
    if (obj instanceof Byte)
      return ((Byte) obj).byteValue();
    if (obj instanceof Short)
      return ((Short) obj).shortValue();
    if (obj instanceof String)
      return Short.valueOf((String) obj).shortValue();
    throw new MessageFormatException("can't convert property value to short");
  }

  char getChar(String name) throws JMSException
  {
    Object obj = getValue(name);

    if (obj == null)
      throw new NullPointerException();
    if (obj instanceof Character)
      return ((Character) obj).charValue();
    throw new MessageFormatException("can't convert message value to char");
  }

  int getInt(String name) throws JMSException
  {
    Object obj = getValue(name);

    if (obj == null)
      return Integer.valueOf((String) obj).intValue();
    if (obj instanceof Byte)
      return ((Byte) obj).intValue();
    if (obj instanceof Short)
      return ((Short) obj).intValue();
    if (obj instanceof Integer)
      return ((Integer) obj).intValue();
    if (obj instanceof String)
      return Integer.valueOf((String) obj).intValue();
    throw new MessageFormatException("can't convert message value to int");
  }

  long getLong(String name) throws JMSException
  {
    Object obj = getValue(name);

    if (obj == null)
      return Long.valueOf((String) obj).longValue();
    if (obj instanceof Byte)
      return ((Byte) obj).intValue();
    if (obj instanceof Short)
      return ((Short) obj).intValue();
    if (obj instanceof Integer)
      return ((Integer) obj).intValue();
    if (obj instanceof Long)
      return ((Long) obj).longValue();
    if (obj instanceof String)
      return Long.valueOf((String) obj).longValue();
    throw new MessageFormatException("can't convert message value to long");
  }

  float getFloat(String name) throws JMSException
  {
    Object obj = getValue(name);

    if (obj == null)
      return Float.valueOf((String) obj).floatValue();
    if (obj instanceof Float)
      return ((Float) obj).floatValue();
    if (obj instanceof String)
      return Float.valueOf((String) obj).floatValue();
    throw new MessageFormatException("can't convert message value to float");
  }

  double getDouble(String name) throws JMSException
  {
    Object obj = getValue(name);

    if (obj == null)
      return Double.valueOf((String) obj).doubleValue();
    if (obj instanceof Double)
      return ((Double) obj).doubleValue();
    if (obj instanceof Float)
      return ((Float) obj).floatValue();
    if (obj instanceof String)
      return Double.valueOf((String) obj).doubleValue();
    throw new MessageFormatException("can't convert message value to double");
  }

  byte[] getBytes(String name) throws JMSException
  {
    Object obj = getValue(name);

    if (obj == null)
      return null;
    if (obj instanceof byte[])
      return ((byte[]) obj);
    throw new MessageFormatException("can't convert message value to byte[]");
  }

  String getString(String name) throws JMSException
  {
    Object obj = getValue(name);

    if (obj == null)
      return null;
    if (obj instanceof byte[])
      throw new MessageFormatException("can't convert byte[] to String");
    return obj.toString();
  }

  Object getObject(String name) throws JMSException
  {
    return getValue(name);
  }

  boolean exists(String name)
  {
    checkMap();
    return map.containsKey(name);
  }

  void remove(String name)
  {
    if (map != null)
      map.remove(name);
  }

  void clear()
  {
    if (map != null)
      map.clear();
  }

  Enumeration enumeration()
  {
    checkMap();
    iterHolder.set(map.keySet().iterator());
    return this;
  }

  public boolean hasMoreElements()
  {
    Iterator iter = (Iterator) iterHolder.get();
    boolean b = iter.hasNext();
    if (!b)
      iterHolder.set(null);
    return b;
  }

  public Object nextElement()
  {
    Iterator iter = (Iterator) iterHolder.get();
    if (iter == null || !iter.hasNext())
      return null;
    return (String) iter.next();
  }

  public String toString()
  {
    return map == null ? "none" : map.toString();
  }
}
