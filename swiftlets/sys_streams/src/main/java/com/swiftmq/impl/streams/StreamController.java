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
import com.swiftmq.impl.streams.graalvm.GraalSetup;
import com.swiftmq.impl.streams.processor.StreamProcessor;
import com.swiftmq.impl.streams.processor.po.POClose;
import com.swiftmq.impl.streams.processor.po.POCollect;
import com.swiftmq.impl.streams.processor.po.POStart;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.deploy.ExtendableClassLoader;
import com.swiftmq.util.SwiftUtilities;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class StreamController {
    static final String REGPREFIX = "repository:";
    SwiftletContext ctx;
    RepositorySupport repositorySupport;
    StreamContext streamContext;
    String domainName;
    String packageName;
    Entity entity;
    String fqn;
    final AtomicBoolean enabled = new AtomicBoolean(false);
    final AtomicBoolean started = new AtomicBoolean(false);
    final AtomicInteger nRestarts = new AtomicInteger();
    final AtomicBoolean initialized = new AtomicBoolean(false);
    final ReentrantLock lock = new ReentrantLock();

    public StreamController(SwiftletContext ctx, RepositorySupport repositorySupport, Entity entity, String domainName, String packageName) {
        this.ctx = ctx;
        this.repositorySupport = repositorySupport;
        this.entity = entity;
        this.domainName = domainName;
        this.packageName = packageName;
        fqn = domainName + "." + packageName + "." + entity.getName();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), this + "/created");
    }

    public String fqn() {
        return fqn;
    }

    private ClassLoader createClassLoader() {
        final ClassLoader classLoader = StreamController.class.getClassLoader();
        File libDir = new File(ctx.streamLibDir + File.separatorChar + fqn);
        if (libDir.exists()) {
            File[] libs = libDir.listFiles((dir, name) -> name.endsWith(".jar"));
            if (libs != null) {
                URL[] urls = new URL[libs.length];
                try {
                    for (int i = 0; i < libs.length; i++)
                        urls[i] = libs[i].toURI().toURL();
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
                ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), "Create classloader for stream: " + fqn + " with libs: " + Arrays.asList(urls));
                return new ExtendableClassLoader(libDir, urls, classLoader);
            }
        }
        return classLoader;
    }

    private String loadScript(String name) throws Exception {
        String script = null;
        if (name.startsWith(REGPREFIX)) {
            script = repositorySupport.get(name);
            if (script == null) {
                throw new Exception("Script '" + name + "' not found in Stream Repository!");
            }
        } else {
            File file = new File(SwiftUtilities.addWorkingDir(name));
            if (!file.exists()) {
                throw new FileNotFoundException("Script file '" + name + "' not found!");
            }
            script = readFileToString(file);
        }
        return script;
    }

    private String readFileToString(File file) throws IOException {
        StringBuilder scriptContent = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                scriptContent.append(line).append("\n");
            }
        }
        return scriptContent.toString();
    }

    private void evalScript() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), this + "/evalScript ...");
        ClassLoader classLoader = createClassLoader();
        streamContext.classLoader = classLoader;
        streamContext.context = GraalSetup.context(classLoader);
        streamContext.bindings = streamContext.context.getBindings("js");
        streamContext.bindings.putMember("stream", streamContext.stream);
        streamContext.bindings.putMember("parameters", new Parameters((EntityList) entity.getEntity("parameters")));
        streamContext.bindings.putMember("time", new TimeSupport());
        streamContext.bindings.putMember("os", new OsSupport());
        streamContext.bindings.putMember("repository", repositorySupport);
        streamContext.bindings.putMember("transform", new ContentTransformer());
        streamContext.bindings.putMember("typeconvert", new TypeConverter());
        streamContext.context.eval("js", loadScript((String) entity.getProperty("script-file").getValue()));
    }

    private void start() throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), this + "/start ...");
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
                streamContext.stream = new Stream(streamContext, domainName, packageName, entity.getName(), nRestarts.get());
                streamContext.messageBuilder = new MessageBuilder(streamContext);
                evalScript();
                streamContext.streamProcessor = new StreamProcessor(streamContext, this);
                Semaphore sem = new Semaphore();
                POStart po = new POStart(sem);
                streamContext.streamProcessor.dispatch(po);
                sem.waitHere(5000);
                if (sem.isNotified() && !po.isSuccess())
                    throw new Exception(po.getException());
                started.set(true);
                ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), "Stream running: " + fqn);
            } catch (Exception e) {
                ctx.usageList.removeEntity(streamContext.usage);
                streamContext.logStackTrace(e);
                throw e;
            }
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), this + "/start done");
        } finally {
            lock.unlock();
        }

    }

    private void stop() {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), this + "/stop ...");
            if (!started.get())
                return;
            try {
                ctx.streamsSwiftlet.stopDependencies(fqn);
            } catch (Exception e) {
                e.printStackTrace();
            }
            ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), "Stopping Stream: " + fqn);
            started.set(false);
            Semaphore sem = new Semaphore();
            streamContext.streamProcessor.dispatch(new POClose(sem));
            sem.waitHere(5000);
            streamContext.streamProcessor.close();
            streamContext.context.close();
            try {
                ctx.usageList.removeEntity(streamContext.usage);
            } catch (EntityRemoveException e) {
            }
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), this + "/stop done");
        } finally {
            lock.unlock();
        }

    }

    public void init() throws Exception {
        if (initialized.get())
            return;
        initialized.set(true);
        streamContext = new StreamContext(ctx, entity);
        Property prop = entity.getProperty("enabled");
        enabled.set((Boolean) prop.getValue());
        prop.setPropertyChangeListener((myProp, oldValue, newValue) -> {
            try {
                boolean b = ((Boolean) newValue).booleanValue();
                if (b && enabled.get() || !b && !enabled.get())
                    return;
                enabled.set(b);
                if (b) {
                    nRestarts.set(0);
                    ctx.streamsSwiftlet.startDependencies(fqn);
                    start();
                } else
                    stop();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), StreamController.this + "/propertyChanged (enabled): " + enabled);
            } catch (Exception e) {
                enabled.set(false);
                throw new PropertyChangeException(e.toString());
            }
        });
        if (enabled.get()) {
            ctx.streamsSwiftlet.startDependencies(fqn);
            start();
        }
    }

    public void restart() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), this + "/restart ...");
        stop();
        long restartDelay = (Long) entity.getProperty("restart-delay").getValue();
        if (restartDelay > 0) {
            long maxRestarts = (Integer) entity.getProperty("restart-max").getValue();
            nRestarts.getAndIncrement();
            if (maxRestarts > nRestarts.get()) {
                ctx.timerSwiftlet.addInstantTimerListener(restartDelay, new TimerListener() {
                    @Override
                    public void performTimeAction() {
                        try {
                            if (enabled.get())
                                start();
                        } catch (Exception e) {
                            ctx.logSwiftlet.logError(toString(), "Exception restarting stream: " + e);
                            stop();
                        }
                    }
                });
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), this + "/restart done");
    }

    public void collect(long interval) {
        if (started.get())
            streamContext.streamProcessor.dispatch(new POCollect(interval));
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), this + "/close ...");
        if (enabled.get())
            stop();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), this + "/close done");
    }

    public String toString() {
        return "StreamController, name=" + domainName + "." + packageName + "." + entity.getName();
    }

    private static final class FallBackClassLoader extends ClassLoader {

        private final ClassLoader firstClassLoader;
        private final ClassLoader secondClassLoader;

        public FallBackClassLoader(ClassLoader firstClassLoader, ClassLoader secondClassLoader) {
            super(null); // Do not set the system class loader as parent
            this.firstClassLoader = firstClassLoader;
            this.secondClassLoader = secondClassLoader;
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            System.err.println("Attempting to load class: " + name);
            // Try the first class loader
            try {
                return firstClassLoader.loadClass(name);
            } catch (ClassNotFoundException ignored) {
                // Ignore and try the second class loader
            }

            // Try the second class loader
            try {
                return secondClassLoader.loadClass(name);
            } catch (ClassNotFoundException ignored) {
                // Ignore and try the application class loader
            }

            // Finally, try the application class loader
            try {
                return ClassLoader.getSystemClassLoader().loadClass(name);
            } catch (ClassNotFoundException e) {
                System.err.println(this + "/Class not found: " + name);
                throw e;
            }
        }
    }

}
