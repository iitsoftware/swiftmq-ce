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

import com.swiftmq.impl.streams.comp.message.MessageBuilder;
import com.swiftmq.impl.streams.processor.StreamProcessor;
import com.swiftmq.impl.streams.processor.po.POClose;
import com.swiftmq.impl.streams.processor.po.POCollect;
import com.swiftmq.impl.streams.processor.po.POStart;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.deploy.ExtendableClassLoader;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleScriptContext;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;

public class StreamController {
    static final String REGPREFIX = "repository:";
    SwiftletContext ctx;
    RepositorySupport repositorySupport;
    StreamContext streamContext;
    String domainName;
    String packageName;
    Entity entity;
    String fqn;
    boolean enabled = false;
    volatile boolean started = false;
    volatile boolean restarting = false;
    volatile int nRestarts = 0;
    boolean initialized = false;

    public StreamController(SwiftletContext ctx, RepositorySupport repositorySupport, Entity entity, String domainName, String packageName) {
        this.ctx = ctx;
        this.repositorySupport = repositorySupport;
        this.entity = entity;
        this.domainName = domainName;
        this.packageName = packageName;
        fqn = domainName + "." + packageName + "." + entity.getName();
        /*${evaltimer}*/
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/created");
    }

    public String fqn() {
        return fqn;
    }

    private ClassLoader createClassLoader() {
        File libDir = new File(ctx.streamLibDir + File.separatorChar + fqn);
        if (libDir.exists()) {
            File[] libs = libDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".jar");
                }
            });
            if (libs != null) {
                URL[] urls = new URL[libs.length];
                try {
                    for (int i = 0; i < libs.length; i++)
                        urls[i] = libs[i].toURI().toURL();
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
                ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), "Create classloader for stream: " + fqn + " with libs: " + Arrays.asList(urls));
                return new ExtendableClassLoader(libDir, urls, StreamController.class.getClassLoader());
            }
        }
        return StreamController.class.getClassLoader();
    }

    private Reader loadScript(String name) throws Exception {
        Reader reader = null;
        if (name.startsWith(REGPREFIX)) {
            String script = repositorySupport.get(name);
            if (script == null)
                throw new Exception("Script '" + name + "' not found in Stream Repository!");
            reader = new StringReader(script);
        } else {
            File file = new File(name);
            if (!file.exists())
                throw new FileNotFoundException("Script file '" + name + "' not found!");
            reader = new FileReader(file);
        }
        return reader;
    }

    private void evalScript() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/evalScript ...");
        ScriptEngineManager manager = new ScriptEngineManager();
        Thread.currentThread().setContextClassLoader(createClassLoader());
        ScriptEngine engine = manager.getEngineByName((String) entity.getProperty("script-language").getValue());
        if (engine == null)
            throw new Exception("Engine for script-language '" + entity.getProperty("script-language").getValue() + "' not found!");
        ScriptContext newContext = new SimpleScriptContext();
        streamContext.engineScope = engine.createBindings();
        streamContext.engineScope.put("stream", streamContext.stream);
        streamContext.engineScope.put("parameters", new Parameters((EntityList) entity.getEntity("parameters")));
        streamContext.engineScope.put("time", new TimeSupport());
        streamContext.engineScope.put("os", new OsSupport());
        streamContext.engineScope.put("repository", repositorySupport);
        streamContext.engineScope.put("transform", new ContentTransformer());
        newContext.setBindings(streamContext.engineScope, ScriptContext.ENGINE_SCOPE);

        engine.eval(loadScript((String) entity.getProperty("script-file").getValue()), newContext);
        Thread.currentThread().setContextClassLoader(null);

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/evalScript done");
    }

    private synchronized void start() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/start ...");
        try {
            ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), "Starting Stream: " + fqn);
            try {
                streamContext.usage = ctx.usageList.createEntity();
                streamContext.usage.setName(domainName + "." + packageName + "." + entity.getName());
                streamContext.usage.createCommands();
                ctx.usageList.addEntity(streamContext.usage);
                streamContext.usage.getProperty("starttime").setValue(new Date().toString());
            } catch (Exception e) {
            }
            streamContext.stream = new Stream(streamContext, domainName, packageName, entity.getName(), nRestarts);
            streamContext.messageBuilder = new MessageBuilder(streamContext);
            evalScript();
            streamContext.streamProcessor = new StreamProcessor(streamContext, this);
            Semaphore sem = new Semaphore();
            POStart po = new POStart(sem);
            streamContext.streamProcessor.dispatch(po);
            sem.waitHere();
            if (!po.isSuccess())
                throw new Exception(po.getException());
            started = true;
        } catch (Exception e) {
            ctx.usageList.removeEntity(streamContext.usage);
            streamContext.logStackTrace(e);
            throw e;
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/start done");
    }

    private synchronized void stop() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/stop ...");
        if (!started)
            return;
        try {
            ctx.streamsSwiftlet.stopDependencies(fqn);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), "Stopping Stream: " + fqn);
        started = false;
        Semaphore sem = new Semaphore();
        streamContext.streamProcessor.dispatch(new POClose(sem));
        sem.waitHere();
        try {
            ctx.usageList.removeEntity(streamContext.usage);
        } catch (EntityRemoveException e) {
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/stop done");
    }

    public void init() throws Exception {
        if (initialized)
            return;
        initialized = true;
        streamContext = new StreamContext(ctx, entity);
        Property prop = entity.getProperty("enabled");
        enabled = ((Boolean) prop.getValue()).booleanValue();
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property myProp, Object oldValue, Object newValue) throws PropertyChangeException {
                try {
                    boolean b = ((Boolean) newValue).booleanValue();
                    if (b && enabled || !b && !enabled)
                        return;
                    enabled = b;
                    if (b) {
                        nRestarts = 0;
                        ctx.streamsSwiftlet.startDependencies(fqn);
                        start();
                    } else
                        stop();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), StreamController.this.toString() + "/propertyChanged (enabled): " + enabled);
                } catch (Exception e) {
                    enabled = false;
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        if (enabled) {
            ctx.streamsSwiftlet.startDependencies(fqn);
            start();
        }
    }

    public void restart() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/restart ...");
        stop();
        long restartDelay = (Long) entity.getProperty("restart-delay").getValue();
        if (restartDelay > 0) {
            long maxRestarts = (Integer) entity.getProperty("restart-max").getValue();
            nRestarts++;
            if (maxRestarts > nRestarts) {
                ctx.timerSwiftlet.addInstantTimerListener(restartDelay, new TimerListener() {
                    @Override
                    public void performTimeAction() {
                        try {
                            if (enabled)
                                start();
                        } catch (Exception e) {
                            ctx.logSwiftlet.logError(toString(), "Exception restarting stream: " + e);
                            stop();
                        }
                    }
                });
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/restart done");
    }

    public void collect(long interval) {
        if (started)
            streamContext.streamProcessor.dispatch(new POCollect(interval));
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/close ...");
        if (enabled)
            stop();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("StreamController, name=" + domainName + "." + packageName + ".");
        sb.append(entity.getName());
        return sb.toString();
    }
}
