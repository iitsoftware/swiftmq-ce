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

package com.swiftmq.jms.smqp.v510;

/**
 * SMQP-Protocol Version 510, Class: SMQPUtil
 * Automatically generated, don't change!
 * Generation Date: Fri Aug 13 16:00:44 CEST 2004
 * (c) 2004, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.*;
import com.swiftmq.jms.v510.ConnectionMetaDataImpl;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SMQPUtil {
    static void write(boolean b, DataOutput out) throws IOException {
        out.writeBoolean(b);
    }

    static boolean read(boolean b, DataInput in) throws IOException {
        return in.readBoolean();
    }

    static void write(int i, DataOutput out) throws IOException {
        out.writeInt(i);
    }

    static int read(int i, DataInput in) throws IOException {
        return in.readInt();
    }

    static void write(long l, DataOutput out) throws IOException {
        out.writeLong(l);
    }

    static long read(long l, DataInput in) throws IOException {
        return in.readLong();
    }

    static void write(byte[] b, DataOutput out) throws IOException {
        out.writeInt(b.length);
        out.write(b);
    }

    static byte[] read(byte[] b, DataInput in) throws IOException {
        byte[] ba = new byte[in.readInt()];
        in.readFully(ba);
        return ba;
    }

    static void write(String s, DataOutput out) throws IOException {
        out.writeUTF(s);
    }

    static String read(String s, DataInput in) throws IOException {
        return in.readUTF();
    }

    static void write(DestinationImpl q, DataOutput out) throws IOException {
        DestinationFactory.dumpDestination(q, out);
    }

    static QueueImpl read(QueueImpl q, DataInput in) throws IOException {
        return (QueueImpl) DestinationFactory.createDestination(in);
    }

    static TopicImpl read(TopicImpl q, DataInput in) throws IOException {
        return (TopicImpl) DestinationFactory.createDestination(in);
    }

    static void write(ConnectionMetaDataImpl c, DataOutput out) throws IOException {
        c.writeContent(out);
    }

    static ConnectionMetaDataImpl read(ConnectionMetaDataImpl c, DataInput in) throws IOException {
        ConnectionMetaDataImpl cmd = new ConnectionMetaDataImpl();
        cmd.readContent(in);
        return cmd;
    }

    static void write(MessageImpl m, DataOutput out) throws IOException {
        m.writeContent(out);
    }

    static MessageImpl read(MessageImpl m, DataInput in) throws IOException {
        MessageImpl message = MessageImpl.createInstance(in.readInt());
        message.readContent(in);
        return message;
    }

    static void write(MessageIndex m, DataOutput out) throws IOException {
        m.writeContent(out);
    }

    static MessageIndex read(MessageIndex m, DataInput in) throws IOException {
        MessageIndex mi = new MessageIndex();
        mi.readContent(in);
        return mi;
    }

    static void write(MessageEntry m, DataOutput out) throws IOException {
        m.writeContent(out);
    }

    static MessageEntry read(MessageEntry m, DataInput in) throws IOException {
        MessageEntry mi = new MessageEntry();
        mi.readContent(in);
        return mi;
    }

    static void write(MessageEntry[] m, DataOutput out) throws IOException {
        out.writeInt(m.length);
        for (int i = 0; i < m.length; i++)
            m[i].writeContent(out);
    }

    static MessageEntry[] read(MessageEntry[] m, DataInput in) throws IOException {
        MessageEntry[] mi = new MessageEntry[in.readInt()];
        for (int i = 0; i < mi.length; i++) {
            MessageEntry entry = new MessageEntry();
            entry.readContent(in);
            mi[i] = entry;
        }
        return mi;
    }

    static void writebytearray(List l, DataOutput out) throws IOException {
        int length = l.size();
        out.writeInt(length);
        for (int i = 0; i < length; i++) {
            byte[] b = (byte[]) l.get(i);
            out.writeInt(b.length);
            out.write(b);
        }
    }

    static List readbytearray(List l, DataInput in) throws IOException {
        int size = in.readInt();
        List list = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            byte[] b = new byte[in.readInt()];
            in.readFully(b);
            list.add(b);
        }
        return list;
    }

    static void write(XidImpl x, DataOutput out) throws IOException {
        x.writeContent(out);
    }

    static XidImpl read(XidImpl x, DataInput in) throws IOException {
        XidImpl xid = new XidImpl();
        xid.readContent(in);
        return xid;
    }

    static void writeXid(List l, DataOutput out) throws IOException {
        int length = l.size();
        out.writeInt(length);
        for (int i = 0; i < length; i++)
            ((XidImpl) l.get(i)).writeContent(out);
    }

    static List readXid(List l, DataInput in) throws IOException {
        int size = in.readInt();
        List list = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            XidImpl xid = new XidImpl();
            xid.readContent(in);
            list.add(xid);
        }
        return list;
    }

    public static boolean isBulk(AsyncMessageDeliveryRequest request) {
        return request.getBulk() != null;
    }


    public static AsyncMessageDeliveryRequest[] createRequests(AsyncMessageDeliveryRequest bulkRequest) {
        MessageEntry[] bulk = bulkRequest.getBulk();
        int recoveryEpoche = bulkRequest.getRecoveryEpoche();
        int listenerId = bulkRequest.getListenerId();
        int sessionDispatchId = bulkRequest.getSessionDispatchId();
        AsyncMessageDeliveryRequest[] requests = new AsyncMessageDeliveryRequest[bulk.length];
        for (int i = 0; i < bulk.length; i++) {
            requests[i] = new AsyncMessageDeliveryRequest(-1, listenerId, bulk[i], null, sessionDispatchId, false, recoveryEpoche);
        }
        return requests;
    }

    public static MessageImpl getMessage(ProduceMessageRequest request) throws Exception {
        MessageImpl msg = request.getSingleMessage();
        if (msg != null)
            return msg;
        DataByteArrayInputStream dbis = new DataByteArrayInputStream(request.getMessageCopy());
        msg = MessageImpl.createInstance(dbis.readInt());
        msg.readContent(dbis);
        return msg;
    }
}
