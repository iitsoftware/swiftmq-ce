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

package com.swiftmq.swiftlet.queue;

import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.QueueImpl;

import javax.jms.DeliveryMode;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

/**
 * A QueueOutputStream is an output stream that is mapped to a queue. Together with
 * a <code>QueueInputStream</code>, it enables queue based streaming.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see QueueInputStream
 */
public class QueueOutputStream extends OutputStream {
    public static String SEQNO = "QIO$SEQNO";
    public static String SIZE = "QIO$SIZE";
    public static String EOF = "QIO$EOF";

    QueueSender queueSender;
    QueueImpl queue;
    QueuePushTransaction transaction = null;
    Hashtable customMsgProp;
    int deliveryMode = DeliveryMode.NON_PERSISTENT;
    BytesMessageImpl currentMsg;
    int chunkSize = 1024;
    int count = 0;
    int seqNo = 0;
    boolean respectFlowControl = true;


    /**
     * Creates a new QueueOutputStream with the default chunk size of 1 KB.
     *
     * @param queueSender queue sender.
     * @param destination queue.
     */
    public QueueOutputStream(QueueSender queueSender, QueueImpl destination) {
        // SBgen: Assign variable
        this.queueSender = queueSender;
        queue = destination;
    }

    /**
     * Creates a new QueueOutputStream with a specific chunk size.
     *
     * @param queueSender queue sender.
     * @param destination queue.
     * @param chunkSize   chunk size.
     */
    public QueueOutputStream(QueueSender queueSender, QueueImpl destination, int chunkSize) {
        // SBgen: Assign variables
        this.queueSender = queueSender;
        this.chunkSize = chunkSize;
        // SBgen: End assign
        queue = destination;
    }


    /**
     * Creates a new QueueOutputStream with a specific chunk size and custom message properties.
     * These properties are set for every message send by this stream.
     *
     * @param queueSender   queue sender.
     * @param destination   queue.
     * @param customMsgProp message properties.
     * @param chunkSize     chunk size.
     */
    public QueueOutputStream(QueueSender queueSender, QueueImpl destination, Hashtable customMsgProp, int chunkSize) {
        // SBgen: Assign variables
        this.queueSender = queueSender;
        this.customMsgProp = customMsgProp;
        this.chunkSize = chunkSize;
        // SBgen: End assign
        queue = destination;
    }

    /**
     * Set the JMS delivery mode for messages sent by this stream.
     * The default delivery mode is NON_PERSISTENT.
     *
     * @param deliveryMode delivery mode.
     */
    public void setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    /**
     * Specifies whether this stream respects the flow control of the underlying queue or not,
     * hence it waits on flush if there is a delay.
     * The default is true
     *
     * @param respectFlowControl respect FlowControl.
     */
    public void setRespectFlowControl(boolean respectFlowControl) {
        this.respectFlowControl = respectFlowControl;
    }


    /**
     * Set the chunk size for messages sent by this stream.
     *
     * @param chunkSize chunk size.
     */
    public void setChunkSize(int chunkSize) {
        // SBgen: Assign variable
        this.chunkSize = chunkSize;
    }


    /**
     * Returns the chunk size.
     *
     * @return chunk size.
     */
    public int getChunkSize() {
        // SBgen: Get variable
        return (chunkSize);
    }

    private void ensureMessage() {
        if (currentMsg == null)
            currentMsg = new BytesMessageImpl();
    }

    /**
     * Writes the specified byte to this output stream. The general
     * contract for <code>write</code> is that one byte is written
     * to the output stream. The byte to be written is the eight
     * low-order bits of the argument <code>b</code>. The 24
     * high-order bits of <code>b</code> are ignored.
     *
     * @param b the <code>byte</code>.
     * @throws IOException if an I/O error occurs. In particular,
     *                     an <code>IOException</code> may be thrown if the
     *                     output stream has been closed.
     */
    public synchronized void write(int b)
            throws IOException {
        try {
            ensureMessage();
            if (count >= chunkSize) {
                flush();
                ensureMessage();
            }
            currentMsg.writeByte((byte) b);
            count++;
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
    }

    /**
     * Flushes this output stream and forces any buffered output bytes
     * to be written out.
     *
     * @throws IOException if an I/O error occurs.
     */
    public synchronized void flush() throws IOException {
        if (currentMsg == null)
            return;
        try {
            if (customMsgProp != null && seqNo == 0) {
                for (Iterator iter = customMsgProp.entrySet().iterator(); iter.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    currentMsg.setObjectProperty((String) entry.getKey(), entry.getValue());
                }
            }
            currentMsg.setJMSDeliveryMode(deliveryMode);
            currentMsg.setJMSDestination(queue);
            currentMsg.setIntProperty(SIZE, count);
            currentMsg.setIntProperty(SEQNO, seqNo);
            transaction = queueSender.createTransaction();
            transaction.putMessage(currentMsg);
            transaction.commit();
            if (respectFlowControl) {
                long delay = queueSender.getFlowControlDelay();
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (Exception ignored) {
                    }
                }
            }
            currentMsg = null;
            seqNo++;
            count = 0;
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
    }

    /**
     * Closes this output stream and releases any system resources
     * associated with this stream. A <code>flush()</code> is performed
     * before closing. The queue sender remains open.
     *
     * @throws IOException if an I/O error occurs.
     */
    public synchronized void close() throws IOException {
        ensureMessage();
        try {
            currentMsg.setBooleanProperty(EOF, true);
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
        flush();
    }
}

