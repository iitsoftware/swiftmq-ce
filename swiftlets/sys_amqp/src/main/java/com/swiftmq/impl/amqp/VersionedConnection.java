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

package com.swiftmq.impl.amqp;

import com.swiftmq.amqp.OutboundHandler;
import com.swiftmq.amqp.ProtocolHeader;
import com.swiftmq.amqp.Writable;
import com.swiftmq.impl.amqp.amqp.v00_09_01.AMQPHandlerFactory;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.net.protocol.amqp.AMQPInputHandler;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.collection.ConcurrentList;
import com.swiftmq.tools.util.DataStreamInputStream;
import com.swiftmq.tools.util.DataStreamOutputStream;
import com.swiftmq.tools.util.LengthCaptureDataInput;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class VersionedConnection implements com.swiftmq.swiftlet.net.InboundHandler, com.swiftmq.net.client.InboundHandler, OutboundHandler {
    public static final String TP_CONNECTIONSVC = "sys$amqp.connection.service";
    public static final String TP_SESSIONSVC = "sys$amqp.session.service";

    SwiftletContext ctx = null;
    Entity usage = null;
    Entity connectionTemplate = null;
    volatile boolean requiresSasl = false;
    volatile boolean saslFinished = false;
    List<Pair> saslHandlerList = new ConcurrentList<>(new ArrayList<>());
    List<Pair> amqpHandlerList = new ConcurrentList<>(new ArrayList<>());
    Connection connection = null;
    OutboundQueue outboundQueue = null;
    DataStreamInputStream dis = new DataStreamInputStream();
    final AtomicReference<AMQPInputHandler> protHandler = new AtomicReference<>();
    final AtomicReference<Handler> delegate = new AtomicReference<>();
    final AtomicReference<ActiveLogin> activeLogin = new AtomicReference<>();
    final AtomicBoolean closed = new AtomicBoolean(false);

    public VersionedConnection(SwiftletContext ctx, Connection connection, Entity usage, boolean requiresSasl, Entity connectionTemplate) {
        this.ctx = ctx;
        this.connection = connection;
        this.usage = usage;
        this.requiresSasl = requiresSasl;
        this.connectionTemplate = connectionTemplate;
        outboundQueue = new OutboundQueue(ctx, ctx.threadpoolSwiftlet.getPool(TP_CONNECTIONSVC), this);
        outboundQueue.startQueue();
    }

    public Entity getUsage() {
        return usage;
    }

    public Entity getConnectionTemplate() {
        return connectionTemplate;
    }

    public Connection getConnection() {
        return connection;
    }

    public ActiveLogin getActiveLogin() {
        return activeLogin.get();
    }

    public String getRemoteHostname() {
        return connection.getHostname();
    }

    public void setOutboundTracer(OutboundTracer outboundTracer) {
        outboundQueue.setOutboundTracer(outboundTracer);
    }

    public void collect(long lastCollect) {
        Handler handler = delegate.get();
        if (handler != null)
            handler.collect(lastCollect);
    }

    public void registerSaslHandlerFactory(ProtocolHeader header, HandlerFactory factory) {
        saslHandlerList.add(new Pair(header, factory));
    }

    public void registerAMQPHandlerFactory(ProtocolHeader header, HandlerFactory factory) {
        amqpHandlerList.add(new Pair(header, factory));
    }

    private HandlerFactory getFactory(ProtocolHeader header, List<Pair> list) {
        for (int i = list.size() - 1; i >= 0; i--) {
            Pair pair = list.get(i);
            if (pair.header.equals(header))
                return pair.factory;
        }
        return null;
    }

    private void sendAndClose(final Connection connection, ProtocolHeader rcvHeader, ProtocolHeader header) throws IOException {
        if (ctx.protSpace.enabled) ctx.protSpace.trace("amqp", toString() + "/SND: " + header);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/wrong header received: " + rcvHeader + ", required: " + header + ", closing connection");
        ctx.logSwiftlet.logError(ctx.amqpSwiftlet.getName(), toString() + "/wrong header received: " + rcvHeader + ", required: " + header + ", closing connection");
        DataStreamOutputStream out = new DataStreamOutputStream(connection.getOutputStream());
        header.writeContent(out);
        out.flush();
        ctx.timerSwiftlet.addInstantTimerListener(500, new TimerListener() {
            public void performTimeAction() {
                ctx.networkSwiftlet.getConnectionManager().removeConnection(connection);
            }
        });
    }

    public void setSaslFinished(boolean saslFinished, ActiveLogin activeLogin) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/setSaslFinished saslFinished: " + saslFinished + ", activeLogin: " + activeLogin);
        this.saslFinished = saslFinished;
        this.activeLogin.set(activeLogin);
        try {
            usage.getProperty("username").setValue(activeLogin.getUserName());
        } catch (Exception e) {
        }
        delegate.set(null);
        protHandler.get().setProtHeaderExpected(saslFinished);
    }

    public void dataAvailable(Connection connection, InputStream inputStream) throws IOException {
        if (protHandler.get() == null) {
            protHandler.set((AMQPInputHandler) connection.getProtocolInputHandler());
        }
        dis.setInputStream(inputStream);
        dataAvailable(dis);
    }

    public void dataAvailable(LengthCaptureDataInput in) {
        if (delegate.get() != null)
            delegate.get().dataAvailable(in);
        else {
            try {
                ProtocolHeader header = new ProtocolHeader();
                header.readContent(in);
                if (ctx.protSpace.enabled) ctx.protSpace.trace("amqp", toString() + "/RCV: " + header);
                if (header.equals(AMQPHandlerFactory.AMQP_INIT)) {
                    protHandler.get().setProtHeaderExpected(false);
                    protHandler.get().setMode091(true);
                    HandlerFactory factory = getFactory(header, amqpHandlerList);
                    if (factory != null) {
                        delegate.set(factory.createHandler(this));
                        try {
                            usage.getProperty("amqp-version").setValue(delegate.get().getVersion());
                        } catch (Exception e) {
                        }

                    } else {
                        sendAndClose(connection, header, ((Pair) amqpHandlerList.get(amqpHandlerList.size() - 1)).header);
                    }
                } else {
                    if (requiresSasl) {
                        if (saslFinished) {
                            HandlerFactory factory = getFactory(header, amqpHandlerList);
                            if (factory != null) {
                                // start AMQP
                                protHandler.get().setProtHeaderExpected(false);
                                delegate.set(factory.createHandler(this));
                                try {
                                    usage.getProperty("amqp-version").setValue(delegate.get().getVersion());
                                } catch (Exception e) {
                                }
                            } else {
                                // send AMQP_INIT, close
                                sendAndClose(connection, header, ((Pair) amqpHandlerList.get(amqpHandlerList.size() - 1)).header);
                            }
                        } else {
                            HandlerFactory factory = getFactory(header, saslHandlerList);
                            if (factory != null) {
                                // start SASL
                                protHandler.get().setProtHeaderExpected(false);
                                delegate.set(factory.createHandler(this));
                            } else {
                                // send SASL_INIT, close
                                sendAndClose(connection, header, ((Pair) saslHandlerList.get(saslHandlerList.size() - 1)).header);
                            }
                        }
                    } else {
                        if (activeLogin.get() == null)
                            activeLogin.set(ctx.authSwiftlet.createActiveLogin("anonymous", "AMQP"));
                        HandlerFactory factory = getFactory(header, amqpHandlerList);
                        if (factory != null) {
                            // start AMQP
                            protHandler.get().setProtHeaderExpected(false);
                            delegate.set(factory.createHandler(this));
                        } else {
                            // send AMQP_INIT, close
                            sendAndClose(connection, header, ((Pair) amqpHandlerList.get(amqpHandlerList.size() - 1)).header);
                        }
                    }
                }
            } catch (Exception e) {
                ctx.networkSwiftlet.getConnectionManager().removeConnection(connection);
            }
        }
    }

    public void send(Writable writable) {
        outboundQueue.enqueue(writable);
    }

    public void close() {
        if (closed.getAndSet(true))
            return;
        Handler handler = delegate.getAndSet(null);
        if (handler != null) {
            handler.close();
        }
        outboundQueue.stopQueue();
    }

    public String toString() {
        return "VersionedConnection, connection=" + connection;
    }

    private static class Pair {
        ProtocolHeader header = null;
        HandlerFactory factory = null;

        private Pair(ProtocolHeader header, HandlerFactory factory) {
            this.header = header;
            this.factory = factory;
        }
    }
}
