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

package com.swiftmq.impl.jms.standard.v750;

import com.swiftmq.jms.smqp.v750.KeepAliveRequest;
import com.swiftmq.jms.smqp.v750.SMQPBulkRequest;
import com.swiftmq.jms.smqp.v750.SMQPFactory;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.NetworkSwiftlet;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.ReplyHandler;
import com.swiftmq.tools.util.DataStreamOutputStream;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class OutboundWriter
        implements ReplyHandler, TimerListener {
    static KeepAliveRequest keepAliveRequest = new KeepAliveRequest();
    NetworkSwiftlet networkSwiftlet = null;
    TimerSwiftlet timerSwiftlet = null;
    ThreadpoolSwiftlet threadpoolSwiftlet = null;
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;
    Connection connection = null;
    JMSConnection jmsConnection = null;
    DataStreamOutputStream outStream;
    EventLoop eventLoop = null;
    final AtomicBoolean closed = new AtomicBoolean(false);

    OutboundWriter(Connection connection, JMSConnection jmsConnection) {
        this.connection = connection;
        this.jmsConnection = jmsConnection;
        this.outStream = new DataStreamOutputStream(connection.getOutputStream());
        networkSwiftlet = (NetworkSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$net");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_PROTOCOL);
        eventLoop = threadpoolSwiftlet.createEventLoop("sys$jms.connection.outbound", new Processor());
    }

    public EventLoop getEventLoop() {
        return eventLoop;
    }

    public void performReply(Reply reply) {
        eventLoop.submit(reply);
    }

    public void performTimeAction() {
        eventLoop.submit(keepAliveRequest);
    }

    public void close() {
        if (closed.getAndSet(true))
            return;
        eventLoop.close();
    }

    private class Processor implements EventProcessor {
        SMQPBulkRequest bulkRequest = new SMQPBulkRequest();

        private void writeObject(Dumpable obj) {

            if (closed.get())
                return;
            if (traceSpace.enabled) traceSpace.trace("smqp", "write object: " + obj);
            try {
                Dumpalizer.dump(outStream, obj);
                outStream.flush();
                if (obj.getDumpId() == SMQPFactory.DID_DISCONNECT_REP) {
                    timerSwiftlet.addInstantTimerListener(10000, new TimerListener() {
                        public void performTimeAction() {
                            networkSwiftlet.getConnectionManager().removeConnection(connection);
                            close();
                        }
                    });
                }
            } catch (Exception e) {
                if (traceSpace.enabled) traceSpace.trace("smqp", "exception write object, exiting!: " + e);
                networkSwiftlet.getConnectionManager().removeConnection(connection); // closes the connection
                close();
            }
        }

        public void process(List<Object> events) {
            if (events.size() == 1)
                writeObject((Dumpable) events.get(0));
            else {
                bulkRequest.dumpables = events.toArray(new Object[0]);
                bulkRequest.len = events.size();
                writeObject(bulkRequest);
                bulkRequest.dumpables = null;
                bulkRequest.len = 0;
            }
        }
    }

}

