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

package jms.funcunified.msg;

import jms.base.SimpleConnectedUnifiedPTPTestCase;

import javax.jms.*;

public class Messages extends SimpleConnectedUnifiedPTPTestCase
{
  public Messages(String name)
  {
    super(name);
  }

  public void testMessageImpl()
  {
    try
    {
      Message msg = qs.createMessage();
      msg.setBooleanProperty("Boolean1", true);
      msg.setByteProperty("Byte1", (byte) 1);
      msg.setDoubleProperty("Double1", 222.22);
      msg.setFloatProperty("Float1", 3F + 01);
      msg.setIntProperty("Int1", 4);
      msg.setLongProperty("Long1", 5);
      msg.setShortProperty("Short1", (short) 6);
      msg.setStringProperty("String1", "StringValue1");
      msg.setObjectProperty("Boolean2", new Boolean(false));
      msg.setObjectProperty("Byte2", new Byte((byte) 7));
      msg.setObjectProperty("Double2", new Double(888.88));
      msg.setObjectProperty("Float2", new Float(9F + 01));
      msg.setObjectProperty("Int2", new Integer(10));
      msg.setObjectProperty("Long2", new Long(11));
      msg.setObjectProperty("Short2", new Short((short) 12));
      msg.setObjectProperty("String2", "StringValue2");
      producer.send(msg);
      msg = consumer.receive();
      assertTrue("Failed Boolean1", msg.getBooleanProperty("Boolean1") == true);
      assertTrue("Failed Byte1", msg.getByteProperty("Byte1") == (byte) 1);
      assertTrue("Failed Double1", msg.getDoubleProperty("Double1") == 222.22);
      assertTrue("Failed Float1", msg.getFloatProperty("Float1") == 3F + 01);
      assertTrue("Failed Int1", msg.getIntProperty("Int1") == 4);
      assertTrue("Failed Long1", msg.getLongProperty("Long1") == 5);
      assertTrue("Failed Short1", msg.getShortProperty("Short1") == (short) 6);
      assertTrue("Failed String1", msg.getStringProperty("String1").equals("StringValue1"));
      assertTrue("Failed Boolean2", msg.getObjectProperty("Boolean2").equals(new Boolean(false)));
      assertTrue("Failed Byte2", msg.getObjectProperty("Byte2").equals(new Byte((byte) 7)));
      assertTrue("Failed Double2", msg.getObjectProperty("Double2").equals(new Double(888.88)));
      assertTrue("Failed Float2", msg.getObjectProperty("Float2").equals(new Float(9F + 01)));
      assertTrue("Failed Int2", msg.getObjectProperty("Int2").equals(new Integer(10)));
      assertTrue("Failed Long2", msg.getObjectProperty("Long2").equals(new Long(11)));
      assertTrue("Failed Short2", msg.getObjectProperty("Short2").equals(new Short((short) 12)));
      assertTrue("Failed String2", msg.getObjectProperty("String2").equals("StringValue2"));
    } catch (Exception e)
    {
      failFast("testMessageImpl failed: " + e);
    }
  }

  public void testBytesMessageImpl()
  {
    try
    {
      BytesMessage msg = qs.createBytesMessage();
      msg.writeBoolean(true);
      msg.writeBoolean(false);
      msg.writeByte((byte) 1);
      msg.writeByte((byte) 255);
      msg.writeBytes(new byte[]{1, 2, 3});
      msg.writeBytes(new byte[]{5, 6, 7, 8, 9, 10}, 1, 2);
      msg.writeChar('A');
      msg.writeChar('B');
      msg.writeDouble(100.10);
      msg.writeFloat(3F + 01);
      msg.writeInt(200);
      msg.writeLong((long) 39872729);
      msg.writeObject(new Double(55.5));
      msg.writeShort((short) 60);
      msg.writeUTF("String1");
      producer.send(msg);
      msg = (BytesMessage) consumer.receive();
      assertTrue("Failed readBoolean (1)", msg.readBoolean());
      assertTrue("Failed readBoolean (2)", !msg.readBoolean());
      assertTrue("Failed readByte (1)", msg.readByte() == (byte) 1);
      assertTrue("Failed readByte (2)", msg.readByte() == (byte) 255);
      byte[] b = new byte[3];
      msg.readBytes(b);
      assertTrue("Failed readBytes (1)", b[0] == (byte) 1 && b[1] == (byte) 2 && b[2] == (byte) 3);
      b = new byte[2];
      msg.readBytes(b);
      assertTrue("Failed readBytes (2)", b[0] == (byte) 6 && b[1] == (byte) 7);
      assertTrue("Failed readChar (1)", msg.readChar() == 'A');
      assertTrue("Failed readChar (2)", msg.readChar() == 'B');
      assertTrue("Failed readDouble", msg.readDouble() == 100.10);
      assertTrue("Failed readFloat", msg.readFloat() == 3F + 01);
      assertTrue("Failed readInt", msg.readInt() == 200);
      assertTrue("Failed readLong", msg.readLong() == (long) 39872729);
      assertTrue("Failed readDouble (2)", msg.readDouble() == 55.5);
      assertTrue("Failed readShort", msg.readShort() == (short) 60);
      assertTrue("Failed readUTF", msg.readUTF().equals("String1"));
    } catch (Exception e)
    {
      failFast("testBytesMessageImpl failed: " + e);
    }
  }

  public void testStreamMessageImpl()
  {
    try
    {
      StreamMessage msg = qs.createStreamMessage();
      msg.writeBoolean(true);
      msg.writeBoolean(false);
      msg.writeByte((byte) 1);
      msg.writeByte((byte) 255);
      msg.writeBytes(new byte[]{1, 2, 3});
      msg.writeBytes(new byte[]{5, 6, 7, 8, 9, 10}, 1, 2);
      msg.writeChar('A');
      msg.writeChar('B');
      msg.writeDouble(100.10);
      msg.writeFloat(3F + 01);
      msg.writeInt(200);
      msg.writeLong((long) 39872729);
      msg.writeObject(new Double(55.5));
      msg.writeShort((short) 60);
      msg.writeString("String1");
      producer.send(msg);
      msg = (StreamMessage) consumer.receive();
      assertTrue("Failed readBoolean (1)", msg.readBoolean());
      assertTrue("Failed readBoolean (2)", !msg.readBoolean());
      assertTrue("Failed readByte (1)", msg.readByte() == (byte) 1);
      assertTrue("Failed readByte (2)", msg.readByte() == (byte) 255);
      byte[] b = new byte[20];
      int len = msg.readBytes(b);
      assertTrue("Failed readBytes (1), invalid length returned: " + len, len == 3);
      assertTrue("Failed readBytes (1)", b[0] == (byte) 1 && b[1] == (byte) 2 && b[2] == (byte) 3);
      len = msg.readBytes(b);
      assertTrue("Failed readBytes (2), invalid length returned: " + len, len == -1);
      len = msg.readBytes(b);
      assertTrue("Failed readBytes (3), invalid length returned: " + len, len == 2);
      assertTrue("Failed readBytes (3)", b[0] == (byte) 6 && b[1] == (byte) 7);
      assertTrue("Failed readChar (1)", msg.readChar() == 'A');
      assertTrue("Failed readChar (2)", msg.readChar() == 'B');
      assertTrue("Failed readDouble", msg.readDouble() == 100.10);
      assertTrue("Failed readFloat", msg.readFloat() == 3F + 01);
      assertTrue("Failed readInt", msg.readInt() == 200);
      assertTrue("Failed readLong", msg.readLong() == (long) 39872729);
      assertTrue("Failed readDouble (2)", msg.readDouble() == 55.5);
      assertTrue("Failed readShort", msg.readShort() == (short) 60);
      assertTrue("Failed readString", msg.readString().equals("String1"));
    } catch (Exception e)
    {
      failFast("testStreamMessageImpl failed: " + e);
    }
  }

  public void testMapMessageImpl()
  {
    try
    {
      MapMessage msg = qs.createMapMessage();
      msg.setBoolean("P1", true);
      msg.setBoolean("P2", false);
      msg.setByte("P3", (byte) 1);
      msg.setByte("P4", (byte) 255);
      msg.setBytes("P5", new byte[]{1, 2, 3});
      msg.setBytes("P6", new byte[]{5, 6, 7, 8, 9, 10}, 1, 2);
      msg.setChar("P7", 'A');
      msg.setChar("P8", 'B');
      msg.setDouble("P9", 100.10);
      msg.setFloat("P10", 3F + 01);
      msg.setInt("P11", 200);
      msg.setLong("P12", (long) 39872729);
      msg.setShort("P13", (short) 60);
      msg.setString("P14", "String1");
      producer.send(msg);
      msg = (MapMessage) consumer.receive();
      assertTrue("Failed getBoolean (1)", msg.getBoolean("P1"));
      assertTrue("Failed getBoolean (2)", !msg.getBoolean("P2"));
      assertTrue("Failed getByte (1)", msg.getByte("P3") == (byte) 1);
      assertTrue("Failed getByte (2)", msg.getByte("P4") == (byte) 255);
      byte[] b = msg.getBytes("P5");
      int len = b.length;
      assertTrue("Failed getBytes (1), invalid length returned: " + len, len == 3);
      assertTrue("Failed getBytes (1)", b[0] == (byte) 1 && b[1] == (byte) 2 && b[2] == (byte) 3);
      b = msg.getBytes("P6");
      len = b.length;
      assertTrue("Failed getBytes (2), invalid length returned: " + len, len == 2);
      assertTrue("Failed getBytes (2)", b[0] == (byte) 6 && b[1] == (byte) 7);
      assertTrue("Failed getChar (1)", msg.getChar("P7") == 'A');
      assertTrue("Failed getChar (2)", msg.getChar("P8") == 'B');
      assertTrue("Failed getDouble", msg.getDouble("P9") == 100.10);
      assertTrue("Failed getFloat", msg.getFloat("P10") == 3F + 01);
      assertTrue("Failed getInt", msg.getInt("P11") == 200);
      assertTrue("Failed getLong", msg.getLong("P12") == (long) 39872729);
      assertTrue("Failed getShort", msg.getShort("P13") == (short) 60);
      assertTrue("Failed getString", msg.getString("P14").equals("String1"));
    } catch (Exception e)
    {
      failFast("testMapMessageImpl failed: " + e);
    }
  }

  public void testObjectMessageImpl()
  {
    try
    {
      ObjectMessage msg = qs.createObjectMessage();
      msg.setObject(new StringBuffer("A string buffer is an object"));
      producer.send(msg);
      msg = (ObjectMessage) consumer.receive();
      assertTrue("Failed getObject", ((StringBuffer) msg.getObject()).toString().equals("A string buffer is an object"));
    } catch (Exception e)
    {
      failFast("testObjectMessageImpl failed: " + e);
    }
  }

  public void testTextMessageImpl()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      msg.setText("A string");
      producer.send(msg);
      msg = (TextMessage) consumer.receive();
      assertTrue("Failed getText", msg.getText().equals("A string"));
    } catch (Exception e)
    {
      failFast("testTextMessageImpl failed: " + e);
    }
  }

  public void testTextMessageImplLargeText()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      StringBuffer b = new StringBuffer();
      for (int i = 0; i < 200000; i++)
        b.append(i);
      String s = b.toString();
      msg.setText(s);
      producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      msg = (TextMessage) consumer.receive();
      assertTrue("Failed getText", msg.getText().equals(s));
    } catch (Exception e)
    {
      failFast("testTextMessageImplLargeText failed: " + e);
    }
  }
}

