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

package com.swiftmq.swiftlet;

import com.swiftmq.client.thread.PoolManager;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.event.KernelStartupListener;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.event.SwiftletManagerListener;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.deploy.Bundle;
import com.swiftmq.tools.deploy.BundleEvent;
import com.swiftmq.tools.deploy.DeployPath;
import com.swiftmq.tools.log.NullPrintStream;
import com.swiftmq.util.SwiftUtilities;
import com.swiftmq.upgrade.UpgradeUtilities;
import com.swiftmq.util.Version;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * The SwiftletManager is the single instance of a SwiftMQ router that is
 * acting as a container for Swiftlets. It is responsible for:
 * <br>
 * <ul>
 * <li>startup/shutdown of the router (halt/reboot)</li>
 * <li>startup/shutdown of Kernel Swiftlets</li>
 * <li>startup/shutdown of Extension Swiftlets</li>
 * <li>load/save of the configuration file</li>
 * </ul>
 * During the startup phase of the router, the SwiftletManager loads
 * the router's configuration file, and starts all Kernel Swiftlets.
 * The startup order is defined in the 'startorder' attribute of
 * the 'router' tag element of the configuration file.
 * <br>
 * For each Swiftlet defined in the startorder attribute, the following
 * tasks are performed:
 * <br>
 * <ul>
 * <li>Instantiating the Swiftlet class from the kernel classloader</li>
 * <li>Invoking the <code>startup</code> method of the Swiftlet. The
 * Swiftlet configuration is passed as parameter.</li>
 * </ul>
 * The router is running after all Kernel Swiftlet's <code>startup</code>
 * methods have been processed without exceptions.
 * <br>
 * <br>
 * A shutdown of a router is processed in the reverse order and the
 * <code>shutdown</code> method of each Swiftlet is invoked.
 * <br>
 * <br>
 * Extension Swiftlets are loaded/unloaded on request through the methods
 * <code>loadExtensionSwiftlet</code> and <code>unloadExtensionSwiftlet</code>.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class SwiftletManager {
    static final String PROP_SHUTDOWN_HOOK = "swiftmq.shutdown.hook";
    static final String PROP_REUSE_KERNEL_CL = "swiftmq.reuse.kernel.classloader";
    static final long PROP_CONFIG_WATCHDOG_INTERVAL = Long.parseLong(System.getProperty("swiftmq.config.watchdog.interval", "0"));
    static SimpleDateFormat fmt = new SimpleDateFormat(".yyyyMMddHHmmssSSS");

    protected static SwiftletManager _instance = null;
    String configFilename = null;
    Document routerConfig = null;
    String routerName = null;
    String workingDirectory = System.getProperty("user.dir");
    String[] kernelSwiftletNames = null;
    Map swiftletTable = null;
    DeployPath dp = null;
    Map bundleTable = null;
    Map listeners = new HashMap();
    Set allListeners = new HashSet();
    Set kernelListeners = new HashSet();
    Map surviveMap = Collections.synchronizedMap(new HashMap());
    RouterMemoryMeter memoryMeter = null;

    LogSwiftlet logSwiftlet = null;
    TraceSwiftlet traceSwiftlet = null;
    TimerSwiftlet timerSwiftlet = null;

    ConfigfileWatchdog configfileWatchdog = null;

    TraceSpace traceSpace = null;
    Object sSemaphore = new Object();
    Object lSemaphore = new Object();
    long memCollectInterval = 10000;
    boolean smartTree = true;
    boolean startup = false;
    boolean rebooting = false;
    boolean workingDirAdded = false;
    boolean registerShutdownHook = Boolean.valueOf(System.getProperty(PROP_SHUTDOWN_HOOK, "true")).booleanValue();
    boolean quietMode = false;
    boolean strippedMode = false;
    boolean doFireKernelStartedEvent = true;
    PrintStream savedSystemOut = System.out;

    Thread shutdownHook = null;

    protected SwiftletManager() {
    }

    /**
     * Returns the singleton instance of the SwiftletManager
     *
     * @return singleton instance
     */
    public static synchronized SwiftletManager getInstance() {
        if (_instance == null)
            _instance = new SwiftletManager();
        return _instance;
    }

    public boolean isHA() {
        return false;
    }

    public void setDoFireKernelStartedEvent(boolean doFireKernelStartedEvent) {
        this.doFireKernelStartedEvent = doFireKernelStartedEvent;
    }

    protected void trace(String message) {
        if (!quietMode && traceSpace != null && traceSpace.enabled)
            traceSpace.trace("SwiftletManager", message);
    }

    protected Configuration getConfiguration(Swiftlet swiftlet) throws Exception {
        trace("Swiftlet " + swiftlet.getName() + "', getConfiguration");
        Configuration config = (Configuration) RouterConfiguration.Singleton().getConfigurations().get(swiftlet.getName());
        if (config == null) {
            trace("Swiftlet " + swiftlet.getName() + "', get configuration template");
            config = getConfigurationTemplate(swiftlet.getName());
            if (config == null)
                throw new Exception("Swiftlet " + swiftlet.getName() + "', getConfigurationTemplate returns null");
            trace("Swiftlet " + swiftlet.getName() + "', fill configuration");
            config.getMetaData().setName(swiftlet.getName());
            config = fillConfiguration(config);
        }
        return config;
    }

    /**
     * Returns the configuration of a specific Swiftlet.
     *
     * @param name Swiftlet name, e.g. "sys$topicmanager".
     * @return configuration
     */
    public Configuration getConfiguration(String name) {
        return (Configuration) RouterConfiguration.Singleton().getConfigurations().get(name);
    }

    private void startUpSwiftlet(Swiftlet swiftlet, Configuration config) throws SwiftletException {
        System.out.println("... startup: " + config.getMetaData().getDisplayName());
        if (swiftlet.isKernel()) {
            trace("Swiftlet " + swiftlet.getName() + "', fireSwiftletManagerEvent: swiftletStartInitiated");
            fireSwiftletManagerEvent(swiftlet.getName(), "swiftletStartInitiated", new SwiftletManagerEvent(this, swiftlet.getName()));
            trace("Swiftlet " + swiftlet.getName() + "', swiftlet.startup()");
        }
        swiftlet.startup(config);
        swiftlet.setState(Swiftlet.STATE_ACTIVE);
        if (swiftlet.isKernel()) {
            trace("Swiftlet " + swiftlet.getName() + "', fireSwiftletManagerEvent: swiftletStarted");
            fireSwiftletManagerEvent(swiftlet.getName(), "swiftletStarted", new SwiftletManagerEvent(this, swiftlet.getName()));
        }
    }

    private class SwiftletShutdown implements Runnable {
        Swiftlet swiftlet = null;
        SwiftletException exception = null;

        public SwiftletShutdown(Swiftlet swiftlet) {
            this.swiftlet = swiftlet;
        }

        public SwiftletException getException() {
            return exception;
        }

        public void run() {
            try {
                if (swiftlet.isKernel()) {
                    trace("Swiftlet " + swiftlet.getName() + "', fireSwiftletManagerEvent: swiftletStopInitiated");
                    fireSwiftletManagerEvent(swiftlet.getName(), "swiftletStopInitiated", new SwiftletManagerEvent(SwiftletManager.this, swiftlet.getName()));
                    trace("Swiftlet " + swiftlet.getName() + "', swiftlet.shutdown()");
                }
                swiftlet.shutdown();
                swiftlet.setState(Swiftlet.STATE_INACTIVE);
                if (swiftlet.isKernel()) {
                    trace("Swiftlet " + swiftlet.getName() + "', fireSwiftletManagerEvent: swiftletStopped");
                    fireSwiftletManagerEvent(swiftlet.getName(), "swiftletStopped", new SwiftletManagerEvent(SwiftletManager.this, swiftlet.getName()));
                }
            } catch (SwiftletException e) {
                exception = e;
            }
        }
    }

    protected void shutdownSwiftlet(Swiftlet swiftlet) throws SwiftletException {
        try {
            Configuration config = getConfiguration(swiftlet);
            System.out.println("... shutdown: " + config.getMetaData().getDisplayName());
        } catch (Exception ignored) {
        }
        SwiftletShutdown ss = new SwiftletShutdown(swiftlet);
        Thread t = new Thread(ss);
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
        }
        if (ss.getException() != null)
            throw ss.getException();
    }

    protected void startKernelSwiftlets() {
        String actSwiftletName = null;
        Swiftlet swiftlet = null;
        long startupTime = -1;
        String className = null;

        try {
            // First: start TraceSwiftlet
            actSwiftletName = "sys$trace";
            swiftlet = null;
            startupTime = -1;
            try {
                swiftlet = (TraceSwiftlet) loadSwiftlet(actSwiftletName);
            } catch (Exception e) {
                System.err.println("Exception occurred while creating TraceSwiftlet instance: " + e.getMessage());
                System.exit(-1);
            }
            swiftlet.setName(actSwiftletName);
            swiftlet.setKernel(true);
            Configuration c = getConfiguration(swiftlet);
            RouterConfiguration.Singleton().getConfigurations().put(actSwiftletName, c);
            startUpSwiftlet(swiftlet, c);
            swiftlet.setStartupTime(System.currentTimeMillis());
            swiftletTable.put(actSwiftletName, swiftlet);
            traceSwiftlet = (TraceSwiftlet) swiftlet;
            traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
            trace("Trace swiftlet '" + actSwiftletName + " has been started");
            trace("Starting kernel swiftlets");

            // Create Extension Swiftlet Deployer
            new SwiftletDeployer();

            // Next: start the other kernel swiftlets
            for (int i = 0; i < kernelSwiftletNames.length; i++) {
                actSwiftletName = kernelSwiftletNames[i];
                startKernelSwiftlet(actSwiftletName, swiftletTable);
            }
        } catch (Exception e) {
            e.printStackTrace();
            trace("Kernel swiftlet: '" + actSwiftletName + "', exception during startup: " + e.getMessage());
            System.err.println("Exception during startup kernel swiftlet '" + actSwiftletName + "': " + e.getMessage());
            System.exit(-1);
        }
        logSwiftlet = (LogSwiftlet) getSwiftlet("sys$log");
        trace("Kernel swiftlets started");
    }

    protected void startKernelSwiftlet(String actSwiftletName, Map table) throws Exception {
        Swiftlet swiftlet;
        long startupTime;
        trace("Starting kernel swiftlet: '" + actSwiftletName + "' ...");
        swiftlet = null;
        startupTime = -1;
        trace("Kernel swiftlet: '" + actSwiftletName + "'");
        try {
            swiftlet = (Swiftlet) loadSwiftlet(actSwiftletName);
        } catch (Exception e) {
            e.printStackTrace();
            trace("Kernel swiftlet: '" + actSwiftletName + "', exception occurred while creating Swiftlet instance: " + e.getMessage());
            System.err.println("Exception occurred while creating Swiftlet instance: " + e.getMessage());
            System.exit(-1);
        }
        swiftlet.setName(actSwiftletName);
        swiftlet.setKernel(true);
        trace("Kernel swiftlet: '" + actSwiftletName + "', startUpSwiftlet ...");
        table.put(actSwiftletName, swiftlet);
        Configuration conf = getConfiguration(swiftlet);
        RouterConfiguration.Singleton().getConfigurations().put(actSwiftletName, conf);
        startUpSwiftlet(swiftlet, conf);
        swiftlet.setStartupTime(System.currentTimeMillis());
        trace("Kernel swiftlet: '" + actSwiftletName + "', is running");
    }

    protected void stopKernelSwiftlets() {
        trace("stopKernelSwiftlets");
        logSwiftlet.logInformation("SwiftletManager", "stopKernelSwiftlets");
        List al = new ArrayList();
        synchronized (sSemaphore) {
            for (int i = kernelSwiftletNames.length - 1; i >= 0; i--) {
                String name = kernelSwiftletNames[i];
                Swiftlet swiftlet = (Swiftlet) swiftletTable.get(name);
                if (swiftlet.getState() == Swiftlet.STATE_ACTIVE) {
                    al.add(swiftlet);
                }
            }
            al.add(swiftletTable.get("sys$trace"));
        }
        for (int i = 0; i < al.size(); i++) {
            Swiftlet swiftlet = (Swiftlet) al.get(i);
            try {
                shutdownSwiftlet(swiftlet);
            } catch (SwiftletException ignored) {
            }
            swiftlet.setStartupTime(-1);
        }
    }

    private void fillSwiftletTable() {
        for (int i = 0; i < kernelSwiftletNames.length; i++)
            swiftletTable.put(kernelSwiftletNames[i], null);
    }

    public String getWorkingDirectory() {
        return workingDirectory;
    }

    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public boolean isRegisterShutdownHook() {
        return registerShutdownHook;
    }

    public void setRegisterShutdownHook(boolean registerShutdownHook) {
        this.registerShutdownHook = registerShutdownHook;
    }

    public void disableShutdownHook() {
        if (shutdownHook != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
            shutdownHook = null;
        }
    }

    public boolean isQuietMode() {
        return quietMode;
    }

    public void setQuietMode(boolean quietMode) {
        this.quietMode = quietMode;
        if (quietMode)
            System.setOut(new NullPrintStream());
        else
            System.setOut(savedSystemOut);
    }

    public boolean isStrippedMode() {
        return strippedMode;
    }

    public void setStrippedMode(boolean strippedMode) {
        this.strippedMode = strippedMode;
    }

    /**
     * Loads a new Extension Swiftlet. Will be used from the Deploy Swiftlet only.
     *
     * @param bundle deployment bundle.
     * @throws Exception on error during load
     */
    public synchronized void loadExtensionSwiftlet(Bundle bundle) throws Exception {
        String name = bundle.getBundleName();
        trace("loadExtensionSwiftlet: '" + name + "' ...");
        bundleTable.put(bundle.getBundleName(), bundle);
        Swiftlet swiftlet = loadSwiftlet(name);
        swiftlet.setName(name);
        long startupTime = -1;

        Configuration config = getConfiguration(swiftlet);
        RouterConfiguration.Singleton().addEntity(config);
        config.setExtension(true);

        trace("Swiftlet: '" + name + "', startUpSwiftlet ...");
        startUpSwiftlet(swiftlet, config);
        startupTime = System.currentTimeMillis();
        swiftlet.setStartupTime(startupTime);
        swiftletTable.put(name, swiftlet);
        swiftlet = null;
        trace("loadExtensionSwiftlet: '" + name + "' DONE.");
    }

    /**
     * Unloads an Extension Swiftlet. Will be used from the Deploy Swiftlet only.
     *
     * @param bundle deployment bundle.
     */
    public synchronized void unloadExtensionSwiftlet(Bundle bundle) {
        String name = bundle.getBundleName();
        trace("unloadExtensionSwiftlet: '" + name + "' ...");
        try {
            Swiftlet swiftlet = (Swiftlet) swiftletTable.get(name);
            if (swiftlet != null)
                shutdownSwiftlet(swiftlet);
            RouterConfiguration.Singleton().removeEntity(RouterConfiguration.Singleton().getEntity(name));
        } catch (Exception ignored) {
        }
        bundleTable.remove(name);
        swiftletTable.remove(name);
        System.gc();
        System.runFinalization();
        trace("unloadExtensionSwiftlet: '" + name + "' DONE.");
    }

    /**
     * Returns true if the router is configured to use a smart management tree to avoid
     * overloading management tools like SwiftMQ Explorer or CLI with management messages
     * when connected to a router running a high load. This is only a hint. Each Swiftlet
     * is responsible which content it puts into the management tree, especially the usage
     * part.
     *
     * @return true/false.
     */
    public boolean isUseSmartTree() {
        return smartTree;
    }

    /**
     * Returns true if the router is within the startup phase.
     *
     * @return true/false.
     */
    public boolean isStartup() {
        return startup;
    }

    /**
     * Returns true if the router is within the reboot phase.
     *
     * @return true/false.
     */
    public boolean isRebooting() {
        return rebooting;
    }

    protected Map createBundleTable(String kernelPath) throws Exception {
        if (kernelPath == null)
            throw new Exception("Missing attribute: kernelpath");
        File f = new File(SwiftUtilities.addWorkingDir(kernelPath));
        if (!f.exists() || !f.isDirectory())
            throw new Exception("Invalid value for 'kernelpath': directory doesn't exists");
        if (dp == null)
            dp = new DeployPath(f, true, getClass().getClassLoader());
        else {
            boolean reuseCL = Boolean.valueOf(System.getProperty(PROP_REUSE_KERNEL_CL, "false")).booleanValue();
            if (reuseCL)
                dp.init();
            else
                dp = new DeployPath(f, true, getClass().getClassLoader());
        }
        BundleEvent[] events = dp.getBundleEvents();
        if (events == null)
            throw new Exception("No Kernel Swiftlets found in 'kernelpath'");
        Map table = new HashMap();
        for (int i = 0; i < events.length; i++) {
            Bundle b = events[i].getBundle();
            table.put(b.getBundleName(), b);
        }
        return table;
    }

    /**
     * Starts the router.
     * This method is called from the bootstrap class only.
     *
     * @param name name of the configuration file.
     * @throws Exception on error.
     */
    public void startRouter(String name) throws Exception {
        if (!workingDirAdded) {
            configFilename = SwiftUtilities.addWorkingDir(name);
            workingDirAdded = true;
        }
        routerConfig = XMLUtilities.createDocument(new FileInputStream(configFilename));
        
        UpgradeUtilities.checkRelease(configFilename, routerConfig);
        Element root = routerConfig.getRootElement();
        parseOptionalConfiguration(root);
        String value = root.attributeValue("startorder");
        if (value == null)
            throw new Exception("Missing attribute: startorder");
        StringTokenizer t = new StringTokenizer(value, " ,:");
        kernelSwiftletNames = new String[t.countTokens()];
        int i = 0;
        while (t.hasMoreTokens())
            kernelSwiftletNames[i++] = t.nextToken();
        if (root.attributeValue("use-smart-tree") != null)
            smartTree = Boolean.valueOf(root.attributeValue("use-smart-tree")).booleanValue();
        else
            smartTree = true;
        if (root.attributeValue("memory-collect-interval") != null)
            memCollectInterval = Long.valueOf(root.attributeValue("memory-collect-interval")).longValue();
        routerName = root.attributeValue("name");

        bundleTable = createBundleTable(root.attributeValue("kernelpath"));

        initSwiftlets();

        timerSwiftlet = (TimerSwiftlet) getSwiftlet("sys$timer");

        if (PROP_CONFIG_WATCHDOG_INTERVAL > 0) {
            configfileWatchdog = new ConfigfileWatchdog(traceSpace, logSwiftlet, configFilename);
            timerSwiftlet.addTimerListener(PROP_CONFIG_WATCHDOG_INTERVAL, configfileWatchdog);
        }

        // shutdown hook
        if (shutdownHook == null && registerShutdownHook) {
            shutdownHook = new Thread() {
                public void run() {
                    shutdown();
                }
            };
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    protected void parseOptionalConfiguration(Element root) {
    }

    protected void createRouterCommands() {
        RouterConfiguration.Singleton().setName(getRouterName());
        RouterConfiguration.Singleton().createCommands(); // for compatibility
        CommandRegistry commandRegistry = new CommandRegistry("current Router's Swiftlet Manager", null);
        RouterConfiguration.Singleton().setCommandRegistry(commandRegistry);
        CommandExecutor rebootExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 1)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'reboot'"};
                String[] result = new String[]{TreeCommands.INFO, "Reboot Launch in 10 Seconds."};
                Thread t = new Thread(Thread.currentThread().getThreadGroup().getParent(), "Reboot Thread") {
                    public void run() {
                        reboot(10000);
                    }
                };
                t.setDaemon(false); // Important: Must be a non-daemon, otherwise the shutdown hook is activated!
                t.setPriority(1); // IMPORTANT!!! MINIMUM PRIORITY IS REQUIRED
                t.start();
                return result;
            }
        };
        Command rebootCommand = new Command("reboot", "reboot", "Reboot the Router", true, rebootExecutor, true, false);
        commandRegistry.addCommand(rebootCommand);
        CommandExecutor haltExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 1)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'halt'"};
                String[] result = new String[]{TreeCommands.INFO, "Router Halt in 10 Seconds."};
                ;
                Thread t = new Thread(Thread.currentThread().getThreadGroup().getParent(), "Halt Thread") {
                    public void run() {
                        try {
                            Thread.sleep(10000);
                        } catch (Exception ignored) {
                        }
                        shutdown(true);
                        System.exit(0);
                    }
                };
                t.setDaemon(false); // Important: Must be a non-daemon, otherwise the shutdown hook is activated!
                t.start();
                t.setPriority(1); // IMPORTANT!!! MINIMUM PRIORITY IS REQUIRED
                return result;
            }
        };
        Command haltCommand = new Command("halt", "halt", "Halt the Router", true, haltExecutor, true, false);
        commandRegistry.addCommand(haltCommand);
        CommandExecutor saveExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length > 1)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'save'"};
                String[] result = null;
                result = saveConfiguration(RouterConfiguration.Singleton());
                return result;
            }
        };
        Command saveCommand = new Command("save", "save", "Save this Router Configuration", true, saveExecutor, true, false);
        commandRegistry.addCommand(saveCommand);
    }

    private synchronized void initSwiftlets() {
        System.out.println("Booting SwiftMQ " + Version.getKernelVersion() + " ...");
        /*${evalprintout}*/
        startup = true;
        swiftletTable = (Map) Collections.synchronizedMap(new HashMap());

        createRouterCommands();

        try {
            Entity envEntity = new Entity(Configuration.ENV_ENTITY,
                    "Router Environment",
                    "Environment of this Router",
                    null);
            envEntity.createCommands();
            RouterConfiguration.Singleton().addEntity(envEntity);

            Property prop = new Property("routername");
            prop.setType(String.class);
            prop.setDisplayName("Router Name");
            prop.setDescription("Name of this Router");
            prop.setValue(getRouterName());
            prop.setRebootRequired(true);
            prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
                public void propertyChanged(Property property, Object oldValue, Object newValue)
                        throws PropertyChangeException {
                    try {
                        if (newValue != null)
                            SwiftUtilities.verifyRouterName((String) newValue);
                    } catch (Exception e) {
                        throw new PropertyChangeException(e.getMessage());
                    }
                }
            });
            envEntity.addProperty(prop.getName(), prop);
            prop = new Property("use-smart-tree");
            prop.setType(Boolean.class);
            prop.setDisplayName("Use Smart Management Tree");
            prop.setDescription("Use Smart Management Tree (reduced Usage Parts)");
            prop.setValue(new Boolean(smartTree));
            prop.setRebootRequired(true);
            envEntity.addProperty(prop.getName(), prop);
            prop = new Property("release");
            prop.setType(String.class);
            prop.setDisplayName("SwiftMQ Release");
            prop.setDescription("SwiftMQ Release");
            prop.setValue(Version.getKernelVersion());
            prop.setReadOnly(true);
            prop.setStorable(false);
            envEntity.addProperty(prop.getName(), prop);
            prop = new Property("hostname");
            prop.setType(String.class);
            prop.setDisplayName("Hostname");
            prop.setDescription("Router's DNS Hostname");
            String localhost = "unknown";
            try {
                localhost = InetAddress.getByName(InetAddress.getLocalHost().getHostAddress()).getHostName();
            } catch (UnknownHostException e) {
                System.err.println("Unable to determine local host name: " + e);
            }
            prop.setValue(localhost);
            prop.setReadOnly(true);
            prop.setStorable(false);
            envEntity.addProperty(prop.getName(), prop);
            prop = new Property("startuptime");
            prop.setType(String.class);
            prop.setDisplayName("Startup Time");
            prop.setDescription("Router's Startup Time");
            prop.setValue(new Date().toString());
            prop.setReadOnly(true);
            prop.setStorable(false);
            envEntity.addProperty(prop.getName(), prop);
            prop = new Property("os");
            prop.setType(String.class);
            prop.setDisplayName("Operating System");
            prop.setDescription("Router's Host OS");
            prop.setValue(System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch") + " ");
            prop.setReadOnly(true);
            prop.setStorable(false);
            envEntity.addProperty(prop.getName(), prop);
            prop = new Property("jre");
            prop.setType(String.class);
            prop.setDisplayName("JRE");
            prop.setDescription("JRE Version");
            prop.setValue(System.getProperty("java.version"));
            prop.setReadOnly(true);
            prop.setStorable(false);
            envEntity.addProperty(prop.getName(), prop);
            prop = new Property("memory-collect-interval");
            prop.setType(Long.class);
            prop.setDisplayName("Memory Collect Interval");
            prop.setDescription("Memory Collect Interval (ms)");
            prop.setValue(new Long(memCollectInterval));
            prop.setReadOnly(false);
            prop.setStorable(false);
            envEntity.addProperty(prop.getName(), prop);
            memoryMeter = new RouterMemoryMeter(prop);
            envEntity.addEntity(memoryMeter.getMemoryList());
        } catch (Exception ignored) {
        }

        fillSwiftletTable();
        startKernelSwiftlets();
        Set keySet = swiftletTable.keySet();
        Iterator iter = keySet.iterator();
        while (iter.hasNext()) {
            String name = (String) iter.next();
            Swiftlet swiftlet = (Swiftlet) swiftletTable.get(name);
            if (swiftlet.getState() == Swiftlet.STATE_ACTIVE) {
                Configuration config = (Configuration) RouterConfiguration.Singleton().getConfigurations().get(name);
                if (config != null) {
                    MetaData meta = config.getMetaData();
                    if (meta != null)
                        logSwiftlet.logInformation("SwiftletManager", "Swiftlet started: " + meta);
                }
            }
        }
        trace("Init swiftlets successful");
        memoryMeter.start();
        startup = false;
        if (doFireKernelStartedEvent)
            fireKernelStartedEvent();
        System.out.println("SwiftMQ " + Version.getKernelVersion() + " is ready.");
    }

    /**
     * Reboots this router without delay. A separate thread is used. The method returns immediately.
     */
    public void reboot() {
        new Thread(new Runnable() {
            public void run() {
                reboot(0);
            }
        }).start();
    }

    /**
     * Reboots this router with delay. A separate thread is NOT used. The method return after the reboot is done.
     * This method call must be used from a separate thread.
     *
     * @param delay A reboot delay in ms
     */
    public void reboot(long delay) {
        rebooting = true;
        try {
            Thread.sleep(delay);
        } catch (Exception ignored) {
        }
        shutdown();
        System.gc();
        try {
            Thread.sleep(5000);
        } catch (Exception ignored) {
        }
        try {
            startRouter(configFilename);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        rebooting = false;
    }

    protected Swiftlet loadSwiftlet(String swiftletName) throws Exception {
        Bundle bundle = (Bundle) bundleTable.get(swiftletName);
        if (bundle == null)
            throw new Exception("No bundle found for Swiftlet '" + swiftletName + "'");
        Document doc = XMLUtilities.createDocument(bundle.getBundleConfig());
        String className = doc.getRootElement().attributeValue("class");
        if (className == null)
            throw new Exception("Missing Attribute 'class' for Swiftlet '" + swiftletName + "'");
        return (Swiftlet) bundle.getBundleLoader().loadClass(className).newInstance();
    }

    /**
     * Returns the Swiftlet with the given name.
     * This method only returns Kernel Swiftlets. It is not possible to get Extension
     * Swiftlets due to different class loaders. If the Swiftlet is undefined or not started,
     * null is returned.
     *
     * @param swiftletName name of the Swiftlet, e.g. "sys$timer".
     * @return Swiftlet.
     */
    public Swiftlet getSwiftlet(String swiftletName) {
        Swiftlet swiftlet = null;
        synchronized (sSemaphore) {
            swiftlet = (Swiftlet) swiftletTable.get(swiftletName);
        }
        if (swiftlet != null && swiftlet.getState() == Swiftlet.STATE_ACTIVE && swiftlet.isKernel())
            return swiftlet;
        return null;
    }

    Swiftlet _getSwiftlet(String swiftletName) {
        Swiftlet swiftlet = null;
        synchronized (sSemaphore) {
            swiftlet = (Swiftlet) swiftletTable.get(swiftletName);
        }
        if (swiftlet != null && swiftlet.getState() == Swiftlet.STATE_ACTIVE)
            return swiftlet;
        return null;
    }

    /**
     * Returns the state of a Swiftlet.
     *
     * @param swiftletName name of the Swiftlet.
     * @return state.
     * @throws UnknownSwiftletException if the Swiftlet is undefined.
     */
    public final int getSwiftletState(String swiftletName) throws UnknownSwiftletException {
        Swiftlet swiftlet = null;
        synchronized (sSemaphore) {
            swiftlet = (Swiftlet) swiftletTable.get(swiftletName);
        }
        if (swiftlet == null)
            throw new UnknownSwiftletException("Swiftlet '" + swiftletName + "' is unknown");
        return swiftlet.getState();
    }

    /**
     * Checks if the Swiftlet is defined.
     *
     * @param swiftletName name of the Swiftlet.
     * @return true/false.
     */
    public final boolean isSwiftletDefined(String swiftletName) {
        boolean b = false;
        synchronized (sSemaphore) {
            b = swiftletTable.containsKey(swiftletName);
        }
        return b;
    }

    private final void stopAllSwiftlets() {
        trace("stopAllSwiftlets");
        logSwiftlet.logInformation("SwiftletManager", "stopAllSwiftlets");
        List al = new ArrayList();
        synchronized (sSemaphore) {
            for (Iterator iter = RouterConfiguration.Singleton().getConfigurations().entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                if (entity instanceof Configuration) {
                    Configuration conf = (Configuration) entity;
                    if (conf.isExtension()) {
                        String name = conf.getName();
                        Swiftlet swiftlet = (Swiftlet) swiftletTable.get(name);
                        if (swiftlet != null && swiftlet.getState() == Swiftlet.STATE_ACTIVE) {
                            al.add(swiftlet);
                        }
                    }
                }
            }
        }
        for (int i = 0; i < al.size(); i++) {
            Swiftlet swiftlet = (Swiftlet) al.get(i);
            trace("stopAllSwiftlets: Stopping swiftlet '" + swiftlet.getName() + "'");
            logSwiftlet.logInformation("SwiftletManager", "stopAllSwiftlets: Stopping swiftlet '" + swiftlet.getName() + "'");
            try {
                shutdownSwiftlet(swiftlet);
            } catch (SwiftletException ignored) {
            }
            swiftlet.setStartupTime(-1);
            trace("stopAllSwiftlets: Swiftlet " + swiftlet.getName() + " has been stopped");
            logSwiftlet.logInformation("SwiftletManager", "stopAllSwiftlets: Swiftlet " + swiftlet.getName() + " has been stopped");
        }
    }

    public void shutdown(boolean removeShutdownHook) {
        if (removeShutdownHook && shutdownHook != null)
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        shutdown();
    }

    /**
     * Performs a shutdown of the router.
     */
    public synchronized void shutdown() {
        System.out.println("Shutdown SwiftMQ " + Version.getKernelVersion() + " ...");
        trace("shutdown");
        if (configfileWatchdog != null)
            timerSwiftlet.removeTimerListener(configfileWatchdog);
        memoryMeter.close();
        stopAllSwiftlets();
        stopKernelSwiftlets();
        listeners.clear();
        allListeners.clear();
        kernelListeners.clear();
        swiftletTable.clear();
        RouterConfiguration.removeInstance();
        PoolManager.reset();
        traceSpace = null;
        logSwiftlet = null;
        System.out.println("Shutdown SwiftMQ " + Version.getKernelVersion() + " DONE.");
    }

    /**
     * Returns the name of this router
     *
     * @return router name.
     */
    public String getRouterName() {
        return routerName;
    }

    /**
     * Saves this router's configuration.
     */
    public void saveConfiguration() {
        saveConfiguration(RouterConfiguration.Singleton());
    }

    protected Element[] getOptionalElements() {
        return null;
    }

    protected String[] saveConfiguration(RouterConfigInstance entity) {
        ArrayList al = new ArrayList();
        al.add(TreeCommands.INFO);
        try {
            String backupFile = configFilename + fmt.format(new Date());
            File file = new File(configFilename);
            file.renameTo(new File(backupFile));
            al.add("Configuration backed up to file '" + backupFile + "'.");
        } catch (Exception e) {
            al.add("Error creating configuration backup: " + e);
        }
        try {
            Document doc = DocumentHelper.createDocument();
            doc.addComment("  SwiftMQ Configuration. Last Save Time: " + new Date() + "  ");
            Element root = DocumentHelper.createElement("router");
            root.addAttribute("name", (String) entity.getEntity(Configuration.ENV_ENTITY).getProperty("routername").getValue());
            root.addAttribute("kernelpath", routerConfig.getRootElement().attributeValue("kernelpath"));
            root.addAttribute("release", Version.getKernelConfigRelease());
            root.addAttribute("startorder", routerConfig.getRootElement().attributeValue("startorder"));
            boolean b = ((Boolean) entity.getEntity(Configuration.ENV_ENTITY).getProperty("use-smart-tree").getValue()).booleanValue();
            if (!b)
                root.addAttribute("use-smart-tree", "false");
            long l = ((Long) entity.getEntity(Configuration.ENV_ENTITY).getProperty("memory-collect-interval").getValue()).longValue();
            if (l != 10000)
                root.addAttribute("memory-collect-interval", String.valueOf(l));
            Element[] optional = getOptionalElements();
            if (optional != null) {
                for (int i = 0; i < optional.length; i++)
                    XMLUtilities.elementToXML(optional[i], root);
            }
            doc.setRootElement(root);
            Map configs = entity.getEntities();
            for (Iterator iter = configs.keySet().iterator(); iter.hasNext(); ) {
                Entity c = (Entity) configs.get((String) iter.next());
                if (c instanceof Configuration)
                    XMLUtilities.configToXML((Configuration) c, root);
            }
            XMLUtilities.writeDocument(doc, configFilename);
            routerConfig = doc;
            al.add("Configuration saved to file '" + configFilename + "'.");
        } catch (Exception e) {
            al.add("Error saving configuration: " + e);
        }
        return (String[]) al.toArray(new String[al.size()]);
    }

    private Configuration getConfigurationTemplate(String swiftletName) throws Exception {
        Bundle bundle = (Bundle) bundleTable.get(swiftletName);
        if (bundle == null)
            return null;
        Configuration c = XMLUtilities.createConfigurationTemplate(bundle.getBundleConfig());
        XMLUtilities.loadIcons(c, bundle.getBundleLoader());
        return c;
    }

    private Configuration fillConfiguration(Configuration template) throws Exception {
        return XMLUtilities.fillConfiguration(template, routerConfig);
    }

    Configuration fillConfigurationFromTemplate(String swiftletName, Document routerConfigDoc) throws Exception {
        Bundle bundle = (Bundle) bundleTable.get(swiftletName);
        if (bundle == null)
            return null;
        Configuration template = XMLUtilities.createConfigurationTemplate(bundle.getBundleConfig());
        return XMLUtilities.fillConfiguration(template, routerConfigDoc);
    }

    /**
     * Adds data to the survive data store.
     * Due to different class loaders of the Extension Swiftlets, it is not possible
     * to store data in static data structures within the Swiftlet that do survive a reboot (shutdown/restart)
     * of a router. Normally, a Swiftlet shouldn't have any data that must survive,
     * but there are some exceptions, e.g. server sockets which should be reused. This
     * kind of data can be registered within the <code>shutdown</code> method of a Swiftlet
     * under some key and fetched for reuse during the <code>startup</code> method.
     *
     * @param key  some key.
     * @param data the data
     */
    public void addSurviveData(String key, Object data) {
        surviveMap.put(key, data);
    }

    /**
     * Removes the survive data, stored under the given key.
     *
     * @param key key.
     */
    public void removeSurviveData(String key) {
        surviveMap.remove(key);
    }

    /**
     * Returns the survive data, stored under the given key.
     *
     * @param key key.
     * @return survive data.
     */
    public Object getSurviveData(String key) {
        return surviveMap.get(key);
    }

    /**
     * Adds a SwiftletManagerListener for a specific Swiftlet.
     *
     * @param swiftletName Swiftlet Name.
     * @param l            Listener.
     */
    public final void addSwiftletManagerListener(String swiftletName, SwiftletManagerListener l) {
        trace("addSwiftletManagerListener: Swiftlet " + swiftletName + "', adding SwiftletManagerListener");
        synchronized (lSemaphore) {
            HashSet qListeners = (HashSet) listeners.get(swiftletName);
            if (qListeners == null) {
                qListeners = new HashSet();
                listeners.put(swiftletName, qListeners);
            }
            qListeners.add(l);
        }
    }

    /**
     * Adds a SwiftletManagerListener for all Swiftlets.
     *
     * @param l Listener.
     */
    public final void addSwiftletManagerListener(SwiftletManagerListener l) {
        trace("addSwiftletManagerListener: adding SwiftletManagerListener");
        synchronized (lSemaphore) {
            allListeners.add(l);
        }
    }

    /**
     * Removes a SwiftletManagerListener for a specific Swiftlet.
     *
     * @param swiftletName Swiftlet Name.
     * @param l            Listener.
     */
    public final void removeSwiftletManagerListener(String swiftletName, SwiftletManagerListener l) {
        trace("removeSwiftletManagerListener: Swiftlet " + swiftletName + "', removing SwiftletManagerListener");
        synchronized (lSemaphore) {
            HashSet qListeners = (HashSet) listeners.get(swiftletName);
            if (qListeners != null) {
                qListeners.remove(l);
                if (qListeners.isEmpty())
                    listeners.put(swiftletName, null);
            }
        }
    }

    /**
     * Removes a SwiftletManagerListener for all Swiftlets.
     *
     * @param l Listener.
     */
    public final void removeSwiftletManagerListener(SwiftletManagerListener l) {
        trace("removeSwiftletManagerListener: removing SwiftletManagerListener");
        synchronized (lSemaphore) {
            allListeners.remove(l);
        }
    }

    /**
     * Adds a KernelStartupListener.
     *
     * @param l Listener.
     */
    public final void addKernelStartupListener(KernelStartupListener l) {
        trace("addKernelStartupListener: adding KernelStartupListener");
        synchronized (lSemaphore) {
            kernelListeners.add(l);
        }
    }

    /**
     * Removes a KernelStartupListener.
     *
     * @param l Listener.
     */
    public final void removeKernelStartupListener(KernelStartupListener l) {
        trace("removeKernelStartupListener: removing KernelStartupListener");
        synchronized (lSemaphore) {
            kernelListeners.remove(l);
        }
    }

    protected void fireKernelStartedEvent() {
        trace("fireKernelStartedEvent");
        Set cloned = null;
        synchronized (lSemaphore) {
            cloned = (Set) ((HashSet) kernelListeners).clone();
        }
        for (Iterator iter = cloned.iterator(); iter.hasNext(); ) {
            ((KernelStartupListener) iter.next()).kernelStarted();
        }
    }

    protected void fireSwiftletManagerEvent(String swiftletName, String methodName, SwiftletManagerEvent evt) {
        trace("fireSwiftletManagerEvent: Swiftlet " + swiftletName + "', method: " + methodName);
        SwiftletManagerListener[] myListeners = null;
        synchronized (lSemaphore) {
            HashSet qListeners = (HashSet) listeners.get(swiftletName);
            if (qListeners != null) {
                myListeners = (SwiftletManagerListener[]) qListeners.toArray(new SwiftletManagerListener[qListeners.size()]);
            }
        }
        if (myListeners != null) {
            for (int i = 0; i < myListeners.length; i++) {
                SwiftletManagerListener l = (SwiftletManagerListener) myListeners[i];
                if (methodName.equals("swiftletStartInitiated"))
                    l.swiftletStartInitiated(evt);
                else if (methodName.equals("swiftletStarted"))
                    l.swiftletStarted(evt);
                else if (methodName.equals("swiftletStopInitiated"))
                    l.swiftletStopInitiated(evt);
                else if (methodName.equals("swiftletStopped"))
                    l.swiftletStopped(evt);
            }
        }
        myListeners = (SwiftletManagerListener[]) allListeners.toArray(new SwiftletManagerListener[allListeners.size()]);
        if (myListeners != null) {
            for (int i = 0; i < myListeners.length; i++) {
                SwiftletManagerListener l = (SwiftletManagerListener) myListeners[i];
                if (methodName.equals("swiftletStartInitiated"))
                    l.swiftletStartInitiated(evt);
                else if (methodName.equals("swiftletStarted"))
                    l.swiftletStarted(evt);
                else if (methodName.equals("swiftletStopInitiated"))
                    l.swiftletStopInitiated(evt);
                else if (methodName.equals("swiftletStopped"))
                    l.swiftletStopped(evt);
            }
        }
    }
}

