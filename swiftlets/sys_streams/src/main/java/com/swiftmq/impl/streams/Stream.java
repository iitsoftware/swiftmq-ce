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

package com.swiftmq.impl.streams;

import com.swiftmq.impl.streams.comp.io.Input;
import com.swiftmq.impl.streams.comp.io.MailServer;
import com.swiftmq.impl.streams.comp.io.Output;
import com.swiftmq.impl.streams.comp.io.TempQueue;
import com.swiftmq.impl.streams.comp.jdbc.JDBCLookup;
import com.swiftmq.impl.streams.comp.log.Log;
import com.swiftmq.impl.streams.comp.management.CLI;
import com.swiftmq.impl.streams.comp.memory.Memory;
import com.swiftmq.impl.streams.comp.memory.MemoryGroup;
import com.swiftmq.impl.streams.comp.message.Message;
import com.swiftmq.impl.streams.comp.timer.Timer;
import com.swiftmq.impl.streams.processor.po.POFunctionCallback;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.swiftlet.SwiftletManager;

import javax.jms.Destination;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * Stream is the entry point for SwiftMQ Streams.
 * <p/>
 * It is passed as a global variable "stream" to Stream Scripts and is used to create and
 * access Stream resources such as Memories, Timers, Inputs etc.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */

public class Stream {
    StreamContext ctx;
    String domainName;
    String packageName;
    String name;
    String fqn;
    Log log;
    CLI cli;
    Map<String, MemoryGroup> memoryGroups = new HashMap<String, MemoryGroup>();
    Map<String, Memory> memories = new HashMap<String, Memory>();
    Map<String, Timer> timers = new HashMap<String, Timer>();
    Map<String, Input> inputs = new HashMap<String, Input>();
    Map<String, Output> outputs = new HashMap<String, Output>();
    Map<String, MailServer> mailservers = new HashMap<String, MailServer>();
    Map<String, JDBCLookup> jdbcLookups = new HashMap<String, JDBCLookup>();
    Map<String, TempQueue> tempQueues = new HashMap<String, TempQueue>();
    Runnable onMessageCallback = null;
    Runnable onStartCallback = null;
    Runnable onStopCallback = null;
    ExceptionCallback exceptionCallback = null;
    Message current;
    public boolean closed = false;
    int restartCount = 0;
    Memory stateMemory = null;

    /**
     * Internal use only
     */
    public Stream(StreamContext ctx, String domainName, String packageName, String name, int restartCount) {
        this.ctx = ctx;
        this.domainName = domainName;
        this.packageName = packageName;
        this.name = name;
        this.fqn = domainName + "." + packageName + "." + name;
        this.restartCount = restartCount;
    }

    /**
     * Internal use
     *
     * @return stream context
     */
    public StreamContext getStreamCtx() {
        return ctx;
    }

    /**
     * Returns the name of the local Router.
     *
     * @return routerName
     */
    public String routerName() {
        return SwiftletManager.getInstance().getRouterName();
    }

    /**
     * Returns the Domain Name of this Stream.
     *
     * @return domain name
     */
    public String domainName() {
        return domainName;
    }

    /**
     * Returns the Package Name of this Stream
     *
     * @return package name
     */
    public String packageName() {
        return packageName;
    }

    /**
     * Returns the name of this stream
     *
     * @return name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the fully qualified Stream name: domain.package.name
     *
     * @return fully qualified name
     */
    public String fullyQualifiedName() {
        return fqn;
    }

    /**
     * Returns the restart count of this stream
     *
     * @return restart count
     */
    public int restartCount() {
        return restartCount;
    }

    /**
     * Returns the stream's Log
     *
     * @return Log
     */
    public Log log() {
        if (log == null)
            log = new Log(ctx);
        return log;
    }

    /**
     * Returns the stream's CLI
     *
     * @return CLI
     */
    public CLI cli() {
        if (cli == null)
            cli = new CLI(ctx);
        return cli;
    }

    /**
     * Returns the working directory of the router
     *
     * @return
     */
    public String getWorkingDirectory() {
        return SwiftletManager.getInstance().getWorkingDirectory();
    }

    /**
     * Returns the Stream's State Memory that is used by MemoryGroups to store their
     * associated Memories.
     *
     * @return stateMemory
     */
    public Memory stateMemory() {
        try {
            if (stateMemory == null) {
                String stateQueue = "state_" + fullyQualifiedName().replace('.', '_');
                cli().exceptionOff()
                        .execute("cc sys$queuemanager/queues")
                        .execute("new " + stateQueue);
                stateMemory = create().memory("$streamstate").queue(stateQueue);
            }
        } catch (Exception e) {
            log().error("Exception while creating stateMemory: " + e.toString());
        }
        return stateMemory;
    }


    /**
     * Internal use only
     */
    public Memory addMemory(String name, Memory memory) {
        memories.put(name, memory);
        try {
            memory.reload();
        } catch (Exception e) {
            ctx.logStackTrace(e);
        }
        return memory;
    }

    /**
     * Internal use only
     */
    public MemoryGroup addMemoryGroup(String name, MemoryGroup memoryGroup) {
        memoryGroups.put(name, memoryGroup);
        return memoryGroup;
    }

    /**
     * Internal use only
     */
    public void removeMemory(Memory memory) {
        memories.remove(memory.name());
    }

    /**
     * Internal use only
     */
    public void removeMemoryGroup(MemoryGroup memoryGroup) {
        memoryGroups.remove(memoryGroup.name());
    }

    /**
     * Internal use only
     */
    public Timer addTimer(String name, Timer timer) {
        timers.put(name, timer);
        return timer;
    }

    /**
     * Internal use only
     */
    public Stream removeTimer(Timer timer) {
        timers.remove(timer.name());
        return this;
    }

    /**
     * Internal use only
     *
     * @exclude
     */
    public Input addInput(String name, Input input) throws Exception {
        inputs.put(name, input);
        return input;
    }

    /**
     * Internal use only
     */
    public Stream removeInput(Input input) {
        inputs.remove(input.getName());
        return this;
    }

    /**
     * Internal use only
     */
    public Output addOutput(String name, Output output) {
        outputs.put(name, output);
        return output;
    }

    /**
     * Internal use only
     */
    public Stream removeOutput(String name) {
        outputs.remove(name);
        return this;
    }

    /**
     * Internal use only
     */
    public MailServer addMailServer(String name, MailServer mailServer) {
        mailservers.put(name, mailServer);
        return mailServer;
    }

    /**
     * Internal use only
     */
    public Stream removeMailServer(String name) {
        mailservers.remove(name);
        return this;
    }

    /**
     * Internal use only
     */
    public JDBCLookup addJDBCLookup(String name, JDBCLookup jdbcLookup) {
        jdbcLookups.put(name, jdbcLookup);
        return jdbcLookup;
    }

    /**
     * Internal use only
     */
    public Stream removeJDBCLookup(String name) {
        jdbcLookups.remove(name);
        return this;
    }

    /**
     * Internal use only
     */
    public TempQueue addTempQueue(String name, TempQueue tempQueue) {
        tempQueues.put(name, tempQueue);
        return tempQueue;
    }

    /**
     * Internal use only
     */
    public Stream removeTempQueue(TempQueue tempQueue) {
        tempQueues.remove(tempQueue.name());
        return this;
    }

    /**
     * Internal use only
     */
    public Timer[] getTimers() {
        if (timers.size() == 0)
            return null;
        Timer[] t = new Timer[timers.size()];
        int i = 0;
        for (Iterator<Map.Entry<String, Timer>> iter = timers.entrySet().iterator(); iter.hasNext(); ) {
            t[i++] = iter.next().getValue();
        }
        return t;
    }

    /**
     * Internal use only
     */
    public Input[] getInputs() {
        if (inputs.size() == 0)
            return null;
        Input[] inputarr = new Input[inputs.size()];
        int i = 0;
        for (Iterator<Map.Entry<String, Input>> iter = inputs.entrySet().iterator(); iter.hasNext(); ) {
            inputarr[i++] = iter.next().getValue();
        }
        return inputarr;
    }

    /**
     * Internal use only
     */
    public Output[] getOutputs() {
        if (outputs.size() == 0)
            return null;
        Output[] outputarr = new Output[outputs.size()];
        int i = 0;
        for (Iterator<Map.Entry<String, Output>> iter = outputs.entrySet().iterator(); iter.hasNext(); ) {
            outputarr[i++] = iter.next().getValue();
        }
        return outputarr;
    }

    /**
     * Internal use only
     */
    public Memory[] getMemories() {
        if (memories.size() == 0)
            return null;
        Memory[] memarr = new Memory[memories.size()];
        int i = 0;
        for (Iterator<Map.Entry<String, Memory>> iter = memories.entrySet().iterator(); iter.hasNext(); ) {
            memarr[i++] = iter.next().getValue();
        }
        return memarr;
    }

    private void deferredMemoryClose() {
        List<Memory> mems = null;
        for (Iterator<Map.Entry<String, Memory>> iter = memories.entrySet().iterator(); iter.hasNext(); ) {
            Memory mem = iter.next().getValue();
            if (mem.isMarkedAsClose()) {
                if (mems == null)
                    mems = new ArrayList<Memory>();
                mems.add(mem);
            }
        }
        if (mems != null) {
            for (int i = 0; i < mems.size(); i++)
                mems.get(i).deferredClose();
        }
    }

    private void deferredMemoryGroupClose() {
        List<MemoryGroup> mems = null;
        for (Iterator<Map.Entry<String, MemoryGroup>> iter = memoryGroups.entrySet().iterator(); iter.hasNext(); ) {
            MemoryGroup mem = iter.next().getValue();
            if (mem.isMarkedAsClose()) {
                if (mems == null)
                    mems = new ArrayList<MemoryGroup>();
                mems.add(mem);
            }
        }
        if (mems != null) {
            for (int i = 0; i < mems.size(); i++)
                mems.get(i).deferredClose();
        }
    }

    /**
     * Internal use only
     */
    public void deferredClose() {
        deferredMemoryClose();
        deferredMemoryGroupClose();
    }

    /**
     * Internal use only
     */
    public MemoryGroup[] getMemoryGroups() {
        if (memoryGroups.size() == 0)
            return null;
        MemoryGroup[] memarr = new MemoryGroup[memoryGroups.size()];
        int i = 0;
        for (Iterator<Map.Entry<String, MemoryGroup>> iter = memoryGroups.entrySet().iterator(); iter.hasNext(); ) {
            memarr[i++] = iter.next().getValue();
        }
        return memarr;
    }

    /**
     * Internal use only
     */
    public MailServer[] getMailservers() {
        if (mailservers.size() == 0)
            return null;
        MailServer[] serverarr = new MailServer[mailservers.size()];
        int i = 0;
        for (Iterator<Map.Entry<String, MailServer>> iter = mailservers.entrySet().iterator(); iter.hasNext(); ) {
            serverarr[i++] = iter.next().getValue();
        }
        return serverarr;
    }

    /**
     * Internal use only
     */
    public JDBCLookup[] getJDBCLookups() {
        if (jdbcLookups.size() == 0)
            return null;
        JDBCLookup[] serverarr = new JDBCLookup[jdbcLookups.size()];
        int i = 0;
        for (Iterator<Map.Entry<String, JDBCLookup>> iter = jdbcLookups.entrySet().iterator(); iter.hasNext(); ) {
            serverarr[i++] = iter.next().getValue();
        }
        return serverarr;
    }

    /**
     * Internal use only
     */
    public TempQueue[] getTempQueues() {
        if (tempQueues.size() == 0)
            return null;
        TempQueue[] serverarr = new TempQueue[tempQueues.size()];
        int i = 0;
        for (Iterator<Map.Entry<String, TempQueue>> iter = tempQueues.entrySet().iterator(); iter.hasNext(); ) {
            serverarr[i++] = iter.next().getValue();
        }
        return serverarr;
    }

    /**
     * Returns a stream builder to create stream resources
     *
     * @return stream builder
     */
    public StreamBuilder create() {
        return new StreamBuilder(ctx);
    }

    /**
     * Returns the Input with the given name.
     *
     * @param name Name of the Input
     * @return Input
     */
    public Input input(String name) {
        return inputs.get(name);
    }

    /**
     * Returns the Input for this TempQueue.
     *
     * @param tempQueue temp queue
     * @return Input
     */
    public Input input(TempQueue tempQueue) {
        return inputs.get(tempQueue.queueName());
    }

    /**
     * Returns the Output with the given name.
     *
     * @param name Name of the Output
     * @return Output
     */
    public Output output(String name) {
        return outputs.get(name);
    }

    /**
     * Returns the JDBCLookup with the given name.
     *
     * @param name Name of the JDBCLookup
     * @return JDBCLookup
     */
    public JDBCLookup jdbcLookup(String name) {
        return jdbcLookups.get(name);
    }

    /**
     * Returns the MailServer with the given name.
     *
     * @param name Name of the MailServer
     * @return MailServer
     */
    public MailServer mailserver(String name) {
        return mailservers.get(name);
    }

    /**
     * Returns the Timer with the given name.
     *
     * @param name Name of the Timer
     * @return Timer
     */
    public Timer timer(String name) {
        return timers.get(name);
    }

    /**
     * Returns the Memory with the given name.
     *
     * @param name Name of the Memory
     * @return Memory
     */
    public Memory memory(String name) {

        return memories.get(name);
    }

    /**
     * Returns the MemoryGroup with the given name.
     *
     * @param name Name of the MemoryGroup
     * @return MemoryGroup
     */
    public MemoryGroup memoryGroup(String name) {
        return memoryGroups.get(name);
    }

    /**
     * Returns the TempQueue with the given name.
     *
     * @param name Name of the TempQueue
     * @return TempQueue
     */
    public TempQueue tempQueue(String name) {
        return tempQueues.get(name);
    }

    /**
     * Factory method to return a new QueueImpl (address)
     *
     * @param name queue name
     * @return QueueImpl
     */
    public QueueImpl queue(String name) {
        return new QueueImpl(name);
    }

    /**
     * Factory method to return a new TopicImpl (address)
     *
     * @param name topic name
     * @return TopicImpl
     */
    public TopicImpl topic(String name) {
        return new TopicImpl(name);
    }

    /**
     * Returns a Destination registered in JNDI under this name.
     *
     * @param name Name at which the Destination is registered
     * @return Destination
     */
    public Destination lookupJNDI(String name) throws Exception {
        return (Destination) ctx.ctx.jndiSwiftlet.getJNDIObject(name);
    }

    /**
     * Closes all Outputs that were not used between the last and this call
     * to this method.
     *
     * @return this
     */
    public Stream purgeOutputs() {
        Output[] outputs = getOutputs();
        if (outputs != null && outputs.length > 0) {
            for (int i = 0; i < outputs.length; i++) {
                if (outputs[i].isDirty())
                    outputs[i].setDirty(false);
                else
                    outputs[i].close();
            }
        }
        return this;
    }

    /**
     * Sets/overwrites the currently processed Message.
     *
     * @param current current Message
     * @return Stream
     */
    public Stream current(Message current) {
        this.current = current;
        return this;
    }

    /**
     * Returns the currently processed Message. This is automatically set
     * from the stream processor before calling onMessage and set to null
     * thereafter.
     *
     * @return current Message
     */
    public Message current() {
        return current;
    }

    /**
     * Sets the onMessage callback.
     *
     * @param runnable callback
     * @return Stream
     */
    public Stream onMessage(Runnable runnable) {
        onMessageCallback = runnable;
        return this;
    }

    /**
     * Sets the onException callback.
     *
     * @param runnable callback
     * @return
     */
    public Stream onException(ExceptionCallback runnable) {
        this.exceptionCallback = runnable;
        return this;
    }

    /**
     * Sets the onStart callback.
     *
     * @param runnable callback
     * @return
     */
    public Stream onStart(Runnable runnable) {
        this.onStartCallback = runnable;
        return this;
    }

    /**
     * Sets the onStop callback.
     *
     * @param runnable callback
     * @return
     */
    public Stream onStop(Runnable runnable) {
        this.onStopCallback = runnable;
        return this;
    }

    /**
     * Executes a function callback in the Stream's event queue.
     * This is the only method to execute asynchronous calls from libraries.
     *
     * @param functionCallback callback
     * @param context          optional context
     * @return this
     */
    public Stream executeCallback(FunctionCallback functionCallback, Object context) {
        ctx.streamProcessor.dispatch(new POFunctionCallback(null, functionCallback, context));
        return this;
    }

    /**
     * Returns the last exception occurred on the stream
     *
     * @return last exception
     */
    public String lastException() {
        return ctx.getLastException() == null ? null : ctx.getLastException().toString();
    }

    /**
     * Returns the formatted stack trace of the last exception occurred on the stream
     *
     * @return stack trace
     */
    public String lastStackTrace() {
        if (ctx.getLastException() == null)
            return null;
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw, true);
        ctx.getLastException().printStackTrace(writer);
        return sw.toString();
    }

    /**
     * Internal use only
     */
    public void executeOnExceptionCallback() {
        try {
            if (exceptionCallback != null)
                exceptionCallback.execute(lastException(), lastStackTrace());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Internal use only
     */
    public void executeOnMessageCallback() throws Exception {
        if (onMessageCallback != null)
            onMessageCallback.run();
    }

    /**
     * Internal use only
     */
    public void executeOnStartCallback() {
        if (onStartCallback != null)
            onStartCallback.run();
    }

    /**
     * Internal use only
     */
    public void executeOnStopCallback() {
        if (onStopCallback != null)
            onStopCallback.run();
    }

    /**
     * Internal use only
     */
    public void collect(long interval) {
        for (Iterator<Map.Entry<String, Input>> iter = inputs.entrySet().iterator(); iter.hasNext(); ) {
            iter.next().getValue().collect(interval);
        }
        for (Iterator<Map.Entry<String, Timer>> iter = timers.entrySet().iterator(); iter.hasNext(); ) {
            iter.next().getValue().collect(interval);
        }
        for (Iterator<Map.Entry<String, Memory>> iter = memories.entrySet().iterator(); iter.hasNext(); ) {
            iter.next().getValue().collect(interval);
        }
        for (Iterator<Map.Entry<String, JDBCLookup>> iter = jdbcLookups.entrySet().iterator(); iter.hasNext(); ) {
            iter.next().getValue().collect(interval);
        }
        for (Iterator<Map.Entry<String, MailServer>> iter = mailservers.entrySet().iterator(); iter.hasNext(); ) {
            iter.next().getValue().collect(interval);
        }
        for (Iterator<Map.Entry<String, Output>> iter = outputs.entrySet().iterator(); iter.hasNext(); ) {
            iter.next().getValue().collect(interval);
        }

    }

    /**
     * Internal use only
     */
    public void start() throws Exception {
        for (Iterator<Map.Entry<String, Input>> iter = inputs.entrySet().iterator(); iter.hasNext(); ) {
            iter.next().getValue().start();
        }
        for (Iterator<Map.Entry<String, Timer>> iter = timers.entrySet().iterator(); iter.hasNext(); ) {
            iter.next().getValue().start();
        }
    }

    /**
     * Internal use only
     */
    public void close() {
        Input[] arr = getInputs();
        if (arr != null) {
            for (int i = 0; i < arr.length; i++)
                arr[i].close();
        }
        Timer[] arr1 = getTimers();
        if (arr1 != null) {
            for (int i = 0; i < arr1.length; i++)
                arr1[i].close();
        }
        MemoryGroup[] mg = getMemoryGroups();
        if (mg != null) {
            for (int i = 0; i < mg.length; i++)
                mg[i].deferredClose();
        }
        Memory[] arr2 = getMemories();
        if (arr2 != null) {
            for (int i = 0; i < arr2.length; i++)
                arr2[i].deferredClose();
        }
        JDBCLookup[] arr3 = getJDBCLookups();
        if (arr3 != null) {
            for (int i = 0; i < arr3.length; i++)
                arr3[i].close();
        }
        MailServer[] arr4 = getMailservers();
        if (arr4 != null) {
            for (int i = 0; i < arr4.length; i++)
                arr4[i].close();
        }
        Output[] arr5 = getOutputs();
        if (arr5 != null) {
            for (int i = 0; i < arr5.length; i++)
                arr5[i].close();
        }
        TempQueue[] arr6 = getTempQueues();
        if (arr6 != null) {
            for (int i = 0; i < arr6.length; i++)
                arr6[i].delete();
        }
        if (log != null)
            log.close();
        closed = true;
    }

    @Override
    public String toString() {
        return "Stream{" +
                "memories=" + memories +
                ", timers=" + timers +
                ", inputs=" + inputs +
                '}';
    }
}
