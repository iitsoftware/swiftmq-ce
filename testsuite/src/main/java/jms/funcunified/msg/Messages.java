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

public class Messages extends SimpleConnectedUnifiedPTPTestCase {
    public Messages(String name) {
        super(name);
    }

    public void testMessageImpl() {
        try {
            Message msg = qs.createMessage();
            msg.setBooleanProperty("Boolean1", true);
            msg.setByteProperty("Byte1", (byte) 1);
            msg.setDoubleProperty("Double1", 222.22);
            msg.setFloatProperty("Float1", 3F + 1);
            msg.setIntProperty("Int1", 4);
            msg.setLongProperty("Long1", 5);
            msg.setShortProperty("Short1", (short) 6);
            msg.setStringProperty("String1", "StringValue1");
            msg.setObjectProperty("Boolean2", false);
            msg.setObjectProperty("Byte2", (byte) 7);
            msg.setObjectProperty("Double2", 888.88);
            msg.setObjectProperty("Float2", 9F + 1);
            msg.setObjectProperty("Int2", 10);
            msg.setObjectProperty("Long2", 11L);
            msg.setObjectProperty("Short2", (short) 12);
            msg.setObjectProperty("String2", "StringValue2");
            producer.send(msg);
            msg = consumer.receive();
            assertTrue("Failed Boolean1", msg.getBooleanProperty("Boolean1"));
            assertEquals("Failed Byte1", 1, msg.getByteProperty("Byte1"));
            assertEquals("Failed Double1", 222.22, msg.getDoubleProperty("Double1"));
            assertEquals("Failed Float1", 3F + 1, msg.getFloatProperty("Float1"));
            assertEquals("Failed Int1", 4, msg.getIntProperty("Int1"));
            assertEquals("Failed Long1", 5, msg.getLongProperty("Long1"));
            assertEquals("Failed Short1", 6, msg.getShortProperty("Short1"));
            assertEquals("Failed String1", "StringValue1", msg.getStringProperty("String1"));
            assertEquals("Failed Boolean2", false, msg.getObjectProperty("Boolean2"));
            assertEquals("Failed Byte2", msg.getObjectProperty("Byte2"), (byte) 7);
            assertEquals("Failed Double2", 888.88, msg.getObjectProperty("Double2"));
            assertEquals("Failed Float2", 9F + 1, msg.getObjectProperty("Float2"));
            assertEquals("Failed Int2", 10, msg.getObjectProperty("Int2"));
            assertEquals("Failed Long2", 11L, msg.getObjectProperty("Long2"));
            assertEquals("Failed Short2", msg.getObjectProperty("Short2"), (short) 12);
            assertEquals("Failed String2", "StringValue2", msg.getObjectProperty("String2"));
        } catch (Exception e) {
            failFast("testMessageImpl failed: " + e);
        }
    }

    public void testBytesMessageImpl() {
        try {
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
            msg.writeFloat(3F + 1);
            msg.writeInt(200);
            msg.writeLong(39872729);
            msg.writeObject(55.5);
            msg.writeShort((short) 60);
            msg.writeUTF("String1");
            producer.send(msg);
            msg = (BytesMessage) consumer.receive();
            assertTrue("Failed readBoolean (1)", msg.readBoolean());
            assertFalse("Failed readBoolean (2)", msg.readBoolean());
            assertEquals("Failed readByte (1)", msg.readByte(), (byte) 1);
            assertEquals("Failed readByte (2)", msg.readByte(), (byte) 255);
            byte[] b = new byte[3];
            msg.readBytes(b);
            assertTrue("Failed readBytes (1)", b[0] == (byte) 1 && b[1] == (byte) 2 && b[2] == (byte) 3);
            b = new byte[2];
            msg.readBytes(b);
            assertTrue("Failed readBytes (2)", b[0] == (byte) 6 && b[1] == (byte) 7);
            assertEquals("Failed readChar (1)", 'A', msg.readChar());
            assertEquals("Failed readChar (2)", 'B', msg.readChar());
            assertEquals("Failed readDouble", 100.10, msg.readDouble());
            assertEquals("Failed readFloat", 3F + 1, msg.readFloat());
            assertEquals("Failed readInt", 200, msg.readInt());
            assertEquals("Failed readLong", msg.readLong(), 39872729);
            assertEquals("Failed readDouble (2)", 55.5, msg.readDouble());
            assertEquals("Failed readShort", msg.readShort(), (short) 60);
            assertEquals("Failed readUTF", "String1", msg.readUTF());
        } catch (Exception e) {
            failFast("testBytesMessageImpl failed: " + e);
        }
    }

    public void testStreamMessageImpl() {
        try {
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
            msg.writeFloat(3F + 1);
            msg.writeInt(200);
            msg.writeLong(39872729);
            msg.writeObject(55.5);
            msg.writeShort((short) 60);
            msg.writeString("String1");
            producer.send(msg);
            msg = (StreamMessage) consumer.receive();
            assertTrue("Failed readBoolean (1)", msg.readBoolean());
            assertFalse("Failed readBoolean (2)", msg.readBoolean());
            assertEquals("Failed readByte (1)", msg.readByte(), (byte) 1);
            assertEquals("Failed readByte (2)", msg.readByte(), (byte) 255);
            byte[] b = new byte[20];
            int len = msg.readBytes(b);
            assertEquals("Failed readBytes (1), invalid length returned: " + len, 3, len);
            assertTrue("Failed readBytes (1)", b[0] == (byte) 1 && b[1] == (byte) 2 && b[2] == (byte) 3);
            len = msg.readBytes(b);
            assertEquals("Failed readBytes (2), invalid length returned: " + len, -1, len);
            len = msg.readBytes(b);
            assertEquals("Failed readBytes (3), invalid length returned: " + len, 2, len);
            assertTrue("Failed readBytes (3)", b[0] == (byte) 6 && b[1] == (byte) 7);
            assertEquals("Failed readChar (1)", 'A', msg.readChar());
            assertEquals("Failed readChar (2)", 'B', msg.readChar());
            assertEquals("Failed readDouble", 100.10, msg.readDouble());
            assertEquals("Failed readFloat", 3F + 1, msg.readFloat());
            assertEquals("Failed readInt", 200, msg.readInt());
            assertEquals("Failed readLong", msg.readLong(), 39872729);
            assertEquals("Failed readDouble (2)", 55.5, msg.readDouble());
            assertEquals("Failed readShort", msg.readShort(), (short) 60);
            assertEquals("Failed readString", "String1", msg.readString());
        } catch (Exception e) {
            failFast("testStreamMessageImpl failed: " + e);
        }
    }

    public void testMapMessageImpl() {
        try {
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
            msg.setFloat("P10", 3F + 1);
            msg.setInt("P11", 200);
            msg.setLong("P12", 39872729);
            msg.setShort("P13", (short) 60);
            msg.setString("P14", "String1");
            producer.send(msg);
            msg = (MapMessage) consumer.receive();
            assertTrue("Failed getBoolean (1)", msg.getBoolean("P1"));
            assertFalse("Failed getBoolean (2)", msg.getBoolean("P2"));
            assertEquals("Failed getByte (1)", msg.getByte("P3"), (byte) 1);
            assertEquals("Failed getByte (2)", msg.getByte("P4"), (byte) 255);
            byte[] b = msg.getBytes("P5");
            int len = b.length;
            assertEquals("Failed getBytes (1), invalid length returned: " + len, 3, len);
            assertTrue("Failed getBytes (1)", b[0] == (byte) 1 && b[1] == (byte) 2 && b[2] == (byte) 3);
            b = msg.getBytes("P6");
            len = b.length;
            assertEquals("Failed getBytes (2), invalid length returned: " + len, 2, len);
            assertTrue("Failed getBytes (2)", b[0] == (byte) 6 && b[1] == (byte) 7);
            assertEquals("Failed getChar (1)", 'A', msg.getChar("P7"));
            assertEquals("Failed getChar (2)", 'B', msg.getChar("P8"));
            assertEquals("Failed getDouble", 100.10, msg.getDouble("P9"));
            assertEquals("Failed getFloat", 3F + 1, msg.getFloat("P10"));
            assertEquals("Failed getInt", 200, msg.getInt("P11"));
            assertEquals("Failed getLong", msg.getLong("P12"), 39872729);
            assertEquals("Failed getShort", msg.getShort("P13"), (short) 60);
            assertEquals("Failed getString", "String1", msg.getString("P14"));
        } catch (Exception e) {
            failFast("testMapMessageImpl failed: " + e);
        }
    }

    public void testObjectMessageImpl() {
        try {
            ObjectMessage msg = qs.createObjectMessage();
            msg.setObject(new StringBuffer("A string buffer is an object"));
            producer.send(msg);
            msg = (ObjectMessage) consumer.receive();
            assertEquals("Failed getObject", "A string buffer is an object", ((StringBuffer) msg.getObject()).toString());
        } catch (Exception e) {
            failFast("testObjectMessageImpl failed: " + e);
        }
    }

    public void testTextMessageImpl() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setText("A string");
            producer.send(msg);
            msg = (TextMessage) consumer.receive();
            assertEquals("Failed getText", "A string", msg.getText());
        } catch (Exception e) {
            failFast("testTextMessageImpl failed: " + e);
        }
    }

    public void testTextMessageImplLargeText() {
        try {
            TextMessage msg = qs.createTextMessage();
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < 200000; i++)
                b.append(i);
            String s = b.toString();
            msg.setText(s);
            producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = (TextMessage) consumer.receive();
            assertEquals("Failed getText", msg.getText(), s);
        } catch (Exception e) {
            failFast("testTextMessageImplLargeText failed: " + e);
        }
    }
}

