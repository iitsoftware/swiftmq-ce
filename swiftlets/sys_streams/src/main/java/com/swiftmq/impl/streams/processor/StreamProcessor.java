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

package com.swiftmq.impl.streams.processor;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.StreamController;
import com.swiftmq.impl.streams.comp.io.Input;
import com.swiftmq.impl.streams.comp.io.ManagementInput;
import com.swiftmq.impl.streams.comp.io.QueueWireTapInput;
import com.swiftmq.impl.streams.comp.message.Message;
import com.swiftmq.impl.streams.processor.po.*;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.pipeline.POObject;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamProcessor implements POStreamVisitor {
    StreamContext ctx;
    StreamController controller;
    final AtomicBoolean closed = new AtomicBoolean(false);
    int msgsProcessed = 0;
    int totalMsg = 0;
    int timeOnMessage = 0;
    AtomicInteger muxId = new AtomicInteger();

    public StreamProcessor(StreamContext ctx, StreamController controller) {
        this.ctx = ctx;
        this.controller = controller;
        this.muxId.set(ctx.ctx.eventLoopMUX.register(event -> ((POObject) event).accept(StreamProcessor.this)));
    }

    public void dispatch(POObject po) {
        if (closed.get()) {
            if (po.getSemaphore() != null)
                po.getSemaphore().notifySingleWaiter();
        } else
            ctx.ctx.eventLoopMUX.submit(this.muxId.get(), po);
    }

    public void close() {
        ctx.ctx.eventLoopMUX.unregister(muxId.get());
    }

    private void handleException(POObject po, Exception e) {
        ctx.logStackTrace(e);
        po.setException(e.getMessage());
        po.setSuccess(false);
        ctx.rollbackTransactions();
        ctx.setLastException(e);
        ctx.stream.executeOnExceptionCallback();
        ctx.ctx.timerSwiftlet.addInstantTimerListener(100, new TimerListener() {
            @Override
            public void performTimeAction() {
                controller.restart();
            }
        });
    }

    @Override
    public void visit(POStart po) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");

        po.setSuccess(true);

        try {
            ctx.stream.start();
            ctx.stream.log().info("Stream started");
            ctx.stream.executeOnStartCallback();
            ctx.commitTransactions();
        } catch (Exception e) {
            handleException(po, e);
        }

        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " done");

    }

    @Override
    public void visit(POWireTapMessage po) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
        po.setSuccess(true);
        msgsProcessed++;

        try {
            QueueWireTapInput input = po.getWireTapInput();
            Message message = po.getMessage();
            input.current(message).executeCallback();
            if (message.isOnMessageEnabled()) {
                long start = System.nanoTime();
                ctx.stream.current(input.current()).executeOnMessageCallback();
                timeOnMessage = (int) (System.nanoTime() - start) / 1000;
            }
            ctx.commitTransactions();
        } catch (Exception e) {
            handleException(po, e);
        }
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();

        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " done");
    }

    @Override
    public void visit(POMgmtMessage po) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
        po.setSuccess(true);
        msgsProcessed++;

        try {
            ManagementInput input = po.getManagementProcessor().getInput();
            Message message = po.getMessage();
            input.current(message).executeCallback();
            if (message.isOnMessageEnabled()) {
                long start = System.nanoTime();
                ctx.stream.current(input.current()).executeOnMessageCallback();
                timeOnMessage = (int) (System.nanoTime() - start) / 1000;
            }
            ctx.commitTransactions();
        } catch (Exception e) {
            handleException(po, e);
        }
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();

        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " done");
    }

    @Override
    public void visit(POMessage po) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
        po.setSuccess(true);
        msgsProcessed++;

        try {
            QueueMessageProcessor messageProcessor = po.getMessageProcessor();
            if (messageProcessor.isValid()) {
                ctx.addTransaction(messageProcessor.getTransaction(), null);
                Input input = messageProcessor.getInput();
                Message message = ctx.messageBuilder.wrap(messageProcessor.getMessage());
                input.current(message).executeCallback();
                if (message.isOnMessageEnabled()) {
                    long start = System.nanoTime();
                    ctx.stream.current(input.current()).executeOnMessageCallback();
                    timeOnMessage = (int) (System.nanoTime() - start) / 1000;
                }
                ctx.commitTransactions();
                messageProcessor.restart();
            }
        } catch (Exception e) {
            handleException(po, e);
        }
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();

        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " done");

    }

    @Override
    public void visit(POFunctionCallback po) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
        po.setSuccess(true);

        try {
            po.getFunctionCallback().execute(po.getContext());
            ctx.commitTransactions();
        } catch (Exception e) {
            handleException(po, e);
        }
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();

        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " done");
    }

    @Override
    public void visit(POExecute po) {
        ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
        po.setSuccess(true);

        try {
            po.getRunnable().run();
            ctx.commitTransactions();
        } catch (Exception e) {
            handleException(po, e);
        }
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();

        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " done");
    }

    @Override
    public void visit(POCollect po) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");

        if ((long) totalMsg + (long) msgsProcessed > Integer.MAX_VALUE)
            totalMsg = 0;
        totalMsg += msgsProcessed;
        try {
            ctx.usage.getProperty("stream-total-processed").setValue(totalMsg);
            ctx.usage.getProperty("stream-processing-rate").setValue((int) (msgsProcessed / ((double) po.getInterval() / 1000.0)));
            ctx.usage.getProperty("stream-last-onmessage-time").setValue(timeOnMessage);
        } catch (Exception e) {
            e.printStackTrace();
        }
        msgsProcessed = 0;
        ctx.stream.collect(po.getInterval());

        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " done");
    }

    @Override
    public void visit(POTimer po) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
        po.setSuccess(true);

        try {
            po.getTimer().executeCallback();
            ctx.commitTransactions();
        } catch (Exception e) {
            handleException(po, e);
        }
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();

        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " done");

    }

    @Override
    public void visit(POClose po) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");

        po.setSuccess(true);

        ctx.stream.executeOnStopCallback();
        try {
            ctx.commitTransactions();
        } catch (Exception e) {
            ctx.stream.log().error("onStop, exception: " + e.toString());
        }
        ctx.stream.log().info("Stream stopped");
        ctx.stream.close();

        closed.set(true);
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();

        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/visit, po=" + po + " done");

    }

    public String toString() {
        StringBuilder sb = new StringBuilder("StreamProcessor, name=");
        sb.append(ctx.entity.getName());
        return sb.toString();
    }

    private class Processor implements EventProcessor {
        @Override
        public void process(List<Object> events) {
            events.forEach(e -> ((POObject) e).accept(StreamProcessor.this));
        }
    }
}
