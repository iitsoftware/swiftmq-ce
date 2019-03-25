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

package com.swiftmq.admin.cli;

import com.swiftmq.admin.cli.event.RouterListener;
import com.swiftmq.admin.mgmt.*;
import com.swiftmq.jms.ReconnectListener;
import com.swiftmq.mgmt.*;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.requestreply.RequestService;
import com.swiftmq.util.SwiftUtilities;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.TerminalBuilder;

import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.*;
import java.net.InetAddress;
import java.util.*;

/**
 * CLI is SwiftMQ's command line interface.
 * <BR><BR>
 * It can be invoked from command line and can also be used for programmatic
 * administration. For latter usage, CLI is to be constructed with a valid and stopped <tt>QueueConnection</tt>.
 * The user of this <tt>QueueConnection</tt> must have full administrator rights which takes
 * place if he has full right for the queue <tt>swiftmqmgmt</tt> of the respective routers or
 * if the authentication is disabled.
 * <BR><BR>
 * Programmatic administration with CLI is the same as invoking it from command line. All
 * CLI commands can be invoked by the <tt>executeCommand</tt> method. There is only
 * an exception regarding commands that return a result as 'ar', 'lc', 'show template'. These
 * commands are not available for programmatic administration. Invoking such a command
 * results in a <tt>CLIException</tt>.
 * <BR><BR>
 * Please refer to the CLI documentation to get involved with the available CLI commands.
 * <BR><BR>
 * <u>Note (1):</u><BR><BR>
 * To detect a connection lost, for example, as a result of a 'reboot' command for the
 * router the <tt>QueueConnection</tt> is connected to, you have to register your <u>own</u> <tt>ExceptionListener</tt>
 * on the <tt>QueueConnection</tt>.<BR><BR>
 * <u>Note (2):</u><BR><BR>
 * CLI can be used only from a single thread of execution. Executing methods from different
 * threads concurrently can result in an unexpected behavior.  <BR><BR>
 * <u>Note (3):</u><BR><BR>
 * If the connection factory from which the CLI JMS connection is created is configured for
 * transparent reconnect (default in SwiftMQ HA Router), CLI reconnects as well. However, it does
 * not restore the last router in use ('sr' command) nor does it restore the last CLI context
 * ('cc' command). This is the responsibility of the application that uses CLI. In order to
 * restore the router and context, an application must catch CLIReconnectedException and
 * issue the last 'sr' and 'cc' commands before retrying the command that leads to the
 * CLIReconnectedException.
 *
 * @author IIT GmbH, Bremen/Germany
 * @since 1.2
 */
public class CLI implements ReconnectListener {
    private static String INIT_FILE_PROP = "swiftmq.cli.init";
    private static String DEFAULT_INIT_FILE = System.getProperty("user.home") + File.separatorChar + ".init.cli";
    ConnectionHolder connectionHolder = null;
    LineReader inReader = null;
    //  BufferedReader inReader = new BufferedReader(new InputStreamReader(System.in));
    BufferedReader scriptReader = null;
    boolean programmatic = false;
    Object waitSem = new Object();
    Object closeSem = new Object();
    String waitFor = null;
    Vector listeners = new Vector();

    Set availableRouters = Collections.synchronizedSet(new TreeSet());
    CommandRegistry commandRegistry = new CommandRegistry("CLI shell", null);
    Endpoint actRouter = null;
    EndpointRegistry endpointRegistry = new EndpointRegistry();
    Map aliases = (Map) Collections.synchronizedMap(new TreeMap());
    PrintWriter outWriter = null;
    boolean reconnected = false;
    boolean substitute = false;
    boolean verbose = false;
    Map vars = new HashMap();

    Semaphore waitForRouters = new Semaphore();

    private CLI() {
        CommandExecutor exitExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 1)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'exit'"};
                close();
                System.exit(-1);
                return null;
            }
        };
        Command exitCommand = new Command("exit", "exit", "Exit CLI", true, exitExecutor);
        commandRegistry.addCommand(exitCommand);
        CommandExecutor switchExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'sr <router>'"};
                if (availableRouters.contains(cmd[1])) {
                    try {
                        Endpoint routerContext = endpointRegistry.get(cmd[1]);
                        if (routerContext == null)
                            routerContext = createEndpoint(cmd[1], false);
                        actRouter = routerContext;
                        commandRegistry.setDefaultCommand(actRouter);
                        vars.put("routername", cmd[1]);
                    } catch (Exception e) {
                        return new String[]{TreeCommands.ERROR, e.toString()};
                    }
                    return null;
                }
                return new String[]{TreeCommands.ERROR, "Router '" + cmd[1] + "' is unknown."};
            }
        };
        Command switchCommand = new Command("sr", "sr <router>", "Switch to Router <router>", true, switchExecutor);
        commandRegistry.addCommand(switchCommand);
        CommandExecutor availExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 1)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'ar'"};
                ArrayList al = new ArrayList();
                al.add(TreeCommands.RESULT);
                al.add("Available Routers:");
                al.add("");
                String[] s = (String[]) availableRouters.toArray(new String[availableRouters.size()]);
                for (int i = 0; i < s.length; i++)
                    al.add(s[i]);
                al.add("");
                return (String[]) al.toArray(new String[al.size()]);
            }
        };
        Command availCommand = new Command("ar", "ar", "Show all available Routers", true, availExecutor);
        commandRegistry.addCommand(availCommand);
        CommandExecutor wrExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length < 2 || cmd.length > 3)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'wr <router> [<timeout millisecs>]'"};
                if (cmd.length == 2)
                    waitForRouter(cmd[1]);
                else
                    waitForRouter(cmd[1], Long.valueOf(cmd[2]));
                return null;
            }
        };
        Command wrCommand = new Command("wr", "wr <router> [<timeout millisecs>]", "Wait for availability of router <router>", true, wrExecutor);
        commandRegistry.addCommand(wrCommand);
        CommandExecutor substituteExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'substitute on | off'"};
                if (cmd[1].equals("on"))
                    substitute = true;
                else if (cmd[1].equals("off"))
                    substitute = false;
                else
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'substitute on | off'"};
                return null;
            }
        };
        Command substituteCommand = new Command("substitute", "substitute on | off", "Substitutes variables in a command", true, substituteExecutor);
        commandRegistry.addCommand(substituteCommand);
        CommandExecutor verboseExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'verbose on | off'"};
                if (cmd[1].equals("on"))
                    verbose = true;
                else if (cmd[1].equals("off"))
                    verbose = false;
                else
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'verbose on | off'"};
                return null;
            }
        };
        Command verboseCommand = new Command("verbose", "verbose on | off", "Displays each CLI script command before execution", true, verboseExecutor);
        commandRegistry.addCommand(verboseCommand);
        CommandExecutor outputExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'output <filename> | console'"};
                try {
                    if (cmd[1].equals("console"))
                        outWriter = null;
                    else {
                        File f = new File(cmd[1]);
                        if (f.exists())
                            f.delete();
                        outWriter = new PrintWriter(new FileWriter(f), true);
                    }
                } catch (IOException e) {
                    return new String[]{TreeCommands.ERROR, e.getMessage()};
                }
                return null;
            }
        };
        Command outputCommand = new Command("output", "output <filename> | console", "Redirect result output", true, outputExecutor);
        commandRegistry.addCommand(outputCommand);
        CommandExecutor echoExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                StringBuffer b = new StringBuffer("");
                if (cmd.length > 1) {
                    for (int i = 1; i < cmd.length; i++) {
                        if (i != 1)
                            b.append(' ');
                        b.append(cmd[i]);
                    }
                }
                return new String[]{TreeCommands.RESULT, b.toString()};
            }
        };
        Command echoCommand = new Command("echo", "echo [<message>]", "Echos a message", true, echoExecutor);
        commandRegistry.addCommand(echoCommand);
        CommandExecutor varExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length > 3)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'var [<variable> [<value>]]'"};
                if (cmd.length == 3)
                    vars.put(cmd[1], cmd[2]);
                else if (cmd.length == 2)
                    vars.remove(cmd[1]);
                else {
                    for (Iterator iter = vars.entrySet().iterator(); iter.hasNext(); ) {
                        Map.Entry entry = (Map.Entry) iter.next();
                        System.out.println(entry.getKey() + "=" + entry.getValue());
                    }
                }
                return null;
            }
        };
        Command varCommand = new Command("var", "var [<variable> [<value>]]", "Lists, sets or deletes variables", true, varExecutor);
        commandRegistry.addCommand(varCommand);
        CommandExecutor asetExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length < 3)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'aset <alias> <command>'"};
                String alias = cmd[1];
                StringBuffer b = new StringBuffer();
                for (int i = 2; i < cmd.length; i++) {
                    if (i != 2)
                        b.append(' ');
                    b.append(cmd[i]);
                }
                aliases.put(alias, b.toString());
                return null;
            }
        };
        Command asetCommand = new Command("aset", "aset <alias> <command>", "Set a command alias", true, asetExecutor);
        commandRegistry.addCommand(asetCommand);
        CommandExecutor adelExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'adel <alias>'"};
                aliases.remove(cmd[1]);
                return null;
            }
        };
        Command adelCommand = new Command("adel", "adel <alias>", "Remove a command alias", true, adelExecutor);
        commandRegistry.addCommand(adelCommand);
        CommandExecutor alistExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length > 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'alist [<alias>]'"};
                System.out.println();
                StringBuffer b = new StringBuffer();
                b.append(SwiftUtilities.fillToLength("Alias", 33));
                b.append("Command");
                System.out.println(b.toString());
                System.out.println(SwiftUtilities.fillLeft("", 72, '-'));
                if (cmd.length == 2) {
                    String value = (String) aliases.get(cmd[1]);
                    if (value == null)
                        System.out.println("Alias not defined.");
                    else {
                        StringBuffer s = new StringBuffer();
                        s.append(SwiftUtilities.fillToLength(cmd[1], 33));
                        s.append(value);
                        System.out.println(s.toString());
                    }
                } else {
                    if (aliases.size() == 0)
                        System.out.println("No aliases defined.");
                    else {
                        for (Iterator iter = aliases.keySet().iterator(); iter.hasNext(); ) {
                            String key = (String) iter.next();
                            String value = (String) aliases.get(key);
                            StringBuffer s = new StringBuffer();
                            s.append(SwiftUtilities.fillToLength(key, 33));
                            s.append(value);
                            System.out.println(s.toString());

                        }
                    }
                }
                System.out.println();
                return null;
            }
        };
        Command alistCommand = new Command("alist", "alist [<alias>]", "List one or all alias(es)", true, alistExecutor);
        commandRegistry.addCommand(alistCommand);
        CommandExecutor asaveExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length > 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'execute [<filename>]'"};
                String filename = null;
                if (cmd.length == 2)
                    filename = cmd[1];
                else
                    filename = System.getProperty("user.home") + File.separatorChar + ".init.cli";
                System.out.println("Saving aliases to file: " + filename);
                try {
                    PrintWriter pw = new PrintWriter(new FileWriter(filename));
                    pw.println("# CLI aliases, saved: " + new Date());
                    for (Iterator iter = aliases.keySet().iterator(); iter.hasNext(); ) {
                        String key = (String) iter.next();
                        String value = (String) aliases.get(key);
                        pw.println("aset " + key + " " + value);
                    }
                    pw.flush();
                    pw.close();
                } catch (IOException e) {
                    return new String[]{TreeCommands.ERROR, "Unable to saves aliases, exception = " + e};
                }
                return null;
            }
        };
        Command asaveCommand = new Command("asave", "asave [<filename>]", "Saves all aliases into a file", true, asaveExecutor);
        commandRegistry.addCommand(asaveCommand);
        CommandExecutor executeExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'execute <filename>'"};
                String filename = cmd[1];
                try {
                    executeScript(filename);
                } catch (Exception e) {
                    return new String[]{TreeCommands.ERROR, "Unable to execute " + filename + ", exception = " + e};
                }
                return null;
            }
        };
        Command executeCommand = new Command("execute", "execute <filename>", "Executes a CLI script", true, executeExecutor);
        commandRegistry.addCommand(executeCommand);
    }

    CLI(String smqpURL, String qcfName, String scriptFile) throws Exception {
        this();
        inReader = LineReaderBuilder.builder()
                .terminal(TerminalBuilder.terminal())
                .build();
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.swiftmq.jndi.InitialContextFactoryImpl");
        env.put(Context.PROVIDER_URL, smqpURL);
        InitialContext ctx = new InitialContext(env);
        connectionHolder = new JMSConnectionHolder((QueueConnectionFactory) ctx.lookup(qcfName));
        ctx.close();
        String filename = System.getProperty(INIT_FILE_PROP, DEFAULT_INIT_FILE);
        if (new File(filename).exists())
            executeScript(filename);
        if (scriptFile != null) {
            try {
                scriptReader = new BufferedReader(new InputStreamReader(new FileInputStream(scriptFile)));
            } catch (Exception e) {
                throw new Exception("Exception opening Script File: " + e.getMessage());
            }
        }
    }

    public CLI(ConnectionHolder connectionHolder, String scriptFile) throws Exception {
        this();
        inReader = LineReaderBuilder.builder()
                .terminal(TerminalBuilder.terminal())
                .build();
        this.connectionHolder = connectionHolder;
        String filename = System.getProperty(INIT_FILE_PROP, DEFAULT_INIT_FILE);
        if (new File(filename).exists())
            executeScript(filename);
        if (scriptFile != null) {
            try {
                scriptReader = new BufferedReader(new InputStreamReader(new FileInputStream(scriptFile)));
            } catch (Exception e) {
                throw new Exception("Exception opening Script File: " + e.getMessage());
            }
        }
    }

    /**
     * Creates a new CLI object and does the necessary setup
     * (creating some sessions, senders and receivers). After the initial
     * setup, CLI will start the connection.
     *
     * @param queueConnection queue connection, should be in stopped mode
     * @throws Exception if anything goes wrong during setup
     */
    public CLI(QueueConnection queueConnection) throws Exception {
        this();
        programmatic = true;
        connectionHolder = new JMSConnectionHolder(queueConnection);
        connectionHolder.start();
        init();
        connectionHolder.addReconnectListener(this);
        String filename = System.getProperty(INIT_FILE_PROP, DEFAULT_INIT_FILE);
        if (new File(filename).exists())
            executeScript(filename);
    }

    public void reconnected(String host, int port) {
        reconnected = true;
        EndpointRegistry old = endpointRegistry;
        endpointRegistry = new EndpointRegistry();
        actRouter = null;
        commandRegistry.setDefaultCommand(null);
        try {
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }
        old.close();
    }

    /**
     * Returns the actual router name, resulting from the last 'sr' command.
     * If no actual router is set, null is returned.
     *
     * @return actual router name or null
     */
    public String getActRouter() {
        return actRouter != null ? actRouter.getRouterName() : null;
    }

    /**
     * Returns the actual context, resulting from the last 'cc' command. If
     * no actual context is set, null is returned. <BR><BR>
     * <u>Example:</u><BR><BR>
     * Say, the last 'cc' command was <tt>cc sys$queuemanager/queues</tt>, then
     * the actual context returned is  <tt>/sys$queuemanager/queues</tt>.
     *
     * @return actual context or null
     */
    public String getActContext() {
        String[] context = actRouter != null ? actRouter.getActContext() : null;
        return context != null ? "/" + SwiftUtilities.concat(context, "/") : null;
    }

    /**
     * Returns a property value from the actual context. If the property name is
     * not defined in the actual context or no actual context is set, a <tt>CLIException</tt>
     * is thrown. The value of the property is always casted to <tt>String</tt>, regardless
     * of the property type. If the property value is null, null is returned.
     * <br><br>
     * <u>Example:</u><br><br>
     * Say, you like to determine the value of the <tt>cache.size</tt> property of
     * the queue <tt>testqueue</tt>. First, you have to set your actual context:<BR><BR>
     * <tt>cli.executeCommand("cc /sys$queuemanager/queues/testqueue");</tt><BR><BR>
     * Next, you can get the property value:<BR><BR>
     * <tt>int cacheSize = Integer.parseInt(cli.getProperty("cache.size"));</tt>
     *
     * @param name name of the property
     * @return value of the property
     * @throws CLIException if no actual context set or property is unknown in the actual context
     */
    public String getContextProperty(String name) throws CLIException {
        String[] context = actRouter != null ? actRouter.getActContext() : null;
        if (context == null)
            throw new CLIException("No actual context set!");

        String[] result = commandRegistry.executeCommand(context, new String[]{TreeCommands.GET_CONTEXT_PROP, name});
        if (result == null)
            throw new CLIException("Property '" + name + "' is unknown in this context!");
        if (result[0].equals(TreeCommands.ERROR)) {
            if (reconnected) {
                reconnected = false;
                throw new CLIReconnectedException(result[1]);
            } else
                throw new CLIException(result[1]);
        }
        return result.length == 2 ? result[1] : null;
    }

    /**
     * Returns an array with names of all entities of the actual context.
     * <br><br>
     * <u>Example:</u><br><br>
     * Say, you like to determine all defined queues. First, you have to set your actual context:<BR><BR>
     * <tt>cli.executeCommand("cc /sys$queuemanager/queues");</tt><BR><BR>
     * Next, you can get the context entities which are the queue names:<BR><BR>
     * <tt>String[] currentQueueNames = cli.getContextEntities();</tt>
     *
     * @return array of entity names
     * @throws CLIException if no actual context set
     */
    public String[] getContextEntities() throws CLIException {
        String[] context = actRouter != null ? actRouter.getActContext() : null;
        if (context == null)
            throw new CLIException("No actual context set!");

        return commandRegistry.executeCommand(context, new String[]{TreeCommands.GET_CONTEXT_ENTITIES});
    }

    /**
     * Returns all currently available router names.
     *
     * @return array of router names
     */
    public String[] getAvailableRouters() {
        return (String[]) availableRouters.toArray(new String[availableRouters.size()]);
    }

    /**
     * Wait for availability of a specific router. If the router is already available, the method
     * returns immediatly. Otherwise it will wait until the router becomes available.
     *
     * @param routerName router name
     */
    public void waitForRouter(String routerName) {
        synchronized (waitSem) {
            waitFor = routerName;
            if (!availableRouters.contains(routerName)) {
                try {
                    if (!programmatic)
                        System.out.println("Waiting for router '" + routerName + "'");
                    waitSem.wait();
                } catch (Exception ignored) {
                }
            }
            waitFor = null;
        }
    }

    /**
     * Wait for availability of a specific router with timeout. If the router is already available, the method
     * returns immediatly. Otherwise it will wait until the router becomes available or the timeout
     * is reached.
     *
     * @param routerName router name
     * @param timeout    timeout value in milliseconds
     */
    public void waitForRouter(String routerName, long timeout) {
        synchronized (waitSem) {
            waitFor = routerName;
            if (!availableRouters.contains(routerName)) {
                try {
                    if (!programmatic)
                        System.out.println("Waiting for router '" + routerName + "' with timeout " + timeout + " ms");
                    waitSem.wait(timeout);
                } catch (Exception ignored) {
                }
            }
            waitFor = null;
        }
    }

    /**
     * Executes a CLI command. Refer to the CLI documentation for an overview
     * and description of all CLI commands. The method throw a <tt>CLIException</tt> if
     * the command returns a result. Therefore, commads such as 'ar', 'sr', 'lc'
     * cannot be invoked via this method. Note also that the command 'exit' will terminate
     * the whole JVM!
     *
     * @param cmd CLI command to be executed
     * @throws CLIException if something goes wrong such as invalid command or command returns a result
     */
    public void executeCommand(String cmd) throws CLIException {
        try {
            List cmdList = SwiftUtilities.parseCLICommandList(cmd);
            for (Iterator iter = cmdList.iterator(); iter.hasNext(); ) {
                String[] parsedCmd = (String[]) iter.next();
                String aliasValue = (String) aliases.get(parsedCmd[0]);
                if (aliasValue != null) {
                    _processAlias(aliasValue, parsedCmd);
                    continue;
                }
                String[] result = commandRegistry.executeCommand(actRouter == null ? null : actRouter.getActContext(), substitute(parsedCmd));
                if (result != null) {
                    if (result[0].equals(TreeCommands.ERROR)) {
                        String s = SwiftUtilities.concat(SwiftUtilities.cutFirst(result), " ");
                        if (reconnected) {
                            reconnected = false;
                            throw new CLIReconnectedException(s);
                        } else
                            throw new CLIException(s);
                    } else if (result[0].equals(TreeCommands.RESULT))
                        throw new CLIException("Invalid method; command returns a result!");
                }
            }
        } catch (CLIException e) {
            throw e;
        } catch (Exception e1) {
            throw new CLIException(e1.getMessage());
        }
    }

    private String[] substitute(String[] cmd) {
        String[] result = cmd;
        if (substitute) {
            for (int i = 0; i < result.length; i++) {
                for (Iterator iter = vars.entrySet().iterator(); iter.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    String name = (String) entry.getKey();
                    String value = (String) entry.getValue();
                    result[i] = SwiftUtilities.substitute(result[i], name, value);
                }
            }
        }
        try {
            if (verbose)
                out(SwiftUtilities.concat(result, " "));
        } catch (Exception e) {
        }
        return result;
    }

    private void _processAlias(String aliasValue, String[] aliasCmd)
            throws Exception {
        String s = aliasValue;
        String[] t = SwiftUtilities.parseCLICommand(s);
        if (t.length == 1) {
            if (aliasCmd.length > 1) {
                StringBuffer b = new StringBuffer(t[0]);
                for (int i = 1; i < aliasCmd.length; i++) {
                    b.append(" ");
                    b.append(aliasCmd[i]);
                }
                s = b.toString();
            } else
                s = aliasValue;
        } else {
            if (aliasCmd.length > 1 && t.length > 1) {
                s = SwiftUtilities.substitute(s, SwiftUtilities.cutFirst(aliasCmd));
                for (int i = 1; i < aliasCmd.length; i++) {
                    s = SwiftUtilities.substitute(s, String.valueOf(i), aliasCmd[i]);
                }
            }
        }
        executeCommand(s);
    }

    /**
     * Add a router listener
     *
     * @param l router listener
     */
    public void addRouterListener(RouterListener l) {
        listeners.addElement(l);
    }

    /**
     * Remove a router listener.
     *
     * @param l router listener
     */
    public void removeRouterListener(RouterListener l) {
        listeners.removeElement(l);
    }

    private void fireRouterEvent(String routerName, boolean available) {
        synchronized (listeners) {
            for (int i = 0; i < listeners.size(); i++) {
                ((RouterListener) listeners.elementAt(i)).onRouterEvent(routerName, available);
            }
        }
    }

    public boolean isProgrammatic() {
        return programmatic;
    }

    public void markRouter(String routerName, boolean available) {
        if (available) {
            availableRouters.add(routerName);
            synchronized (waitSem) {
                if (waitFor != null && waitFor.equals(routerName))
                    waitSem.notify();
            }
            if (programmatic) {
                fireRouterEvent(routerName, true);
            } else {
                System.out.println("Router '" + routerName + "' is available for administration.");
                waitForRouters.notifySingleWaiter();
            }
        } else {
            availableRouters.remove(routerName);
            removeEndpoint(routerName);
            if (programmatic)
                fireRouterEvent(routerName, false);
            else
                System.out.println("Router '" + routerName + "' is unavailable for administration.");
        }
    }

    public Endpoint createEndpoint(String routerName, boolean routeInfos) throws Exception {
        Endpoint endpoint = connectionHolder.createEndpoint(routerName, new RequestServiceFactory() {
            public RequestService createRequestService(int protocolVersion) {
                if (protocolVersion == 750)
                    return new com.swiftmq.admin.cli.v750.RequestProcessor(CLI.this);
                return new com.swiftmq.admin.cli.v400.RequestProcessor(CLI.this);
            }
        }, programmatic);
        endpoint.setRouteInfos(routeInfos);
        String name = null;
        endpoint.connect(0, InetAddress.getLocalHost().getHostName(), "CLI" + (programmatic ? " Admin API" : ""), routeInfos, false, false);
        endpoint.setStarted(true);
        name = endpoint.getRouterName();
        try {
            endpointRegistry.put(endpoint.getRouterName(), endpoint);
        } catch (EndpointRegistryClosedException e) {
            endpoint.close();
            endpoint = null;
        }
        return endpoint;
    }

    public void removeEndpoint(String routerName) {
        Endpoint routerContext = endpointRegistry.remove(routerName);
        if (routerContext != null) {
            if (routerContext == actRouter) {
                actRouter = null;
                commandRegistry.setDefaultCommand(null);
            }
            routerContext.close();
        }
    }

    private void init() throws Exception {
        Endpoint endpoint = createEndpoint(null, true);
        markRouter(endpoint.getRouterName(), true);
    }

    private String readLine(boolean nullOnEmpty, boolean isPassword) {
        String line = null;
        boolean ok = false;
        try {
            while (!ok) {
                if (scriptReader != null) {
                    line = scriptReader.readLine();
                    if (line == null) {
                        scriptReader.close();
                        scriptReader = null;
                    }
                }
                if (line == null)
                    line = isPassword ? inReader.readLine(new Character('*')) : inReader.readLine();
                ok = line != null && !line.trim().startsWith("#") || line == null;
            }
            if (nullOnEmpty && line != null && line.equals(""))
                line = null;
        } catch (Exception e) {
        }
        return line;
    }

    private void out(String line) throws Exception {
        if (outWriter == null)
            System.out.println(line);
        else
            outWriter.println(line);
    }

    private void executeScript(String filename)
            throws Exception {
        vars.put("scriptpath", new File(filename).getParent());
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (!line.startsWith("#") && line.trim().length() > 0)
                    processCommand(line);
            }
        } finally {
            vars.remove("scriptpath");
        }
    }

    private void processAlias(String aliasValue, String[] aliasCmd)
            throws Exception {
        String s = aliasValue;
        String[] t = SwiftUtilities.parseCLICommand(s);
        if (t.length == 1) {
            if (aliasCmd.length > 1) {
                StringBuffer b = new StringBuffer(t[0]);
                for (int i = 1; i < aliasCmd.length; i++) {
                    b.append(" ");
                    b.append(aliasCmd[i]);
                }
                s = b.toString();
            } else
                s = aliasValue;
        } else {
            if (aliasCmd.length > 1 && t.length > 1) {
                s = SwiftUtilities.substitute(s, SwiftUtilities.cutFirst(aliasCmd));
                for (int i = 1; i < aliasCmd.length; i++) {
                    s = SwiftUtilities.substitute(s, String.valueOf(i), aliasCmd[i]);
                }
            }
        }
        processCommand(s.trim());
    }

    private void processCommand(String command)
            throws Exception {
        List cmdList = SwiftUtilities.parseCLICommandList(command);
        for (Iterator iter = cmdList.iterator(); iter.hasNext(); ) {
            String[] parsedCmd = (String[]) iter.next();
            parsedCmd[0] = parsedCmd[0].trim();
            String aliasValue = (String) aliases.get(parsedCmd[0]);
            if (aliasValue != null) {
                processAlias(aliasValue, parsedCmd);
                continue;
            }
            String[] result = commandRegistry.executeCommand(actRouter == null ? null : actRouter.getActContext(), substitute(parsedCmd));
            if (result != null) {
                if (result[0].equals(TreeCommands.ERROR) && result.length > 1) {
                    System.out.println(result[1]);
                } else {
                    for (int i = 0; i < result.length; i++) {
                        if (!(result[i].equals(TreeCommands.RESULT) ||
                                result[i].equals(TreeCommands.INFO)))
                            out(result[i]);
                    }
                }
            }
        }
    }

    private void mainLoop() {
        String prompt = null;
        String[] context = null;
        boolean shouldExit = false;
        while (!shouldExit) {
            if (actRouter != null) {
                prompt = actRouter.getRouterName();
                if (actRouter.getActContext() != null)
                    prompt += '/' + SwiftUtilities.concat(actRouter.getActContext(), "/");
                context = actRouter.getActContext();
            } else
                prompt = "";
            prompt += "> ";
            if (scriptReader == null)
                System.out.print(prompt);
            String command = readLine(false, false);
            if (command == null) {
                System.out.println("Bye.");
                return;
            }
            command = command.trim();
            if (command.length() > 0) {
                try {
                    processCommand(command);
                } catch (Exception e) {
                    System.out.println();
                    System.out.println(e.getMessage());
                    System.out.println();
                }
            }
        }
    }

    public void run() {
        String userName = null;
        String password = null;
        if (scriptReader == null) {
            System.out.println();
            System.out.println("Welcome to SwiftMQ!");
            System.out.println();
            System.out.print("Username: ");
            userName = readLine(true, false);
            System.out.print("Password: ");
            password = readLine(true, true);
        } else {
            userName = System.getProperty("cli.username");
            password = System.getProperty("cli.password");
        }
        if (scriptReader == null)
            System.out.print("Trying to connect ... ");
        try {
            connectionHolder.connect(userName, password);
            if (scriptReader == null)
                System.out.println("connected");
        } catch (Exception e) {
            System.out.println("failed, exception: " + e.getMessage());
            close();
            return;
        }

        try {
            connectionHolder.setExceptionListener(new ExceptionListener() {
                public void onException(Exception e) {
                    if (e.getMessage() == null)
                        System.out.println("Connection lost.");
                    else
                        System.out.println("Exception occurred: " + e.getMessage());
                    System.exit(-1);
                }
            });
            connectionHolder.start();
            init();
            connectionHolder.addReconnectListener(this);
        } catch (Exception e) {
            System.out.println("failed to establish route listener, exception: " + e.getMessage());
            close();
            return;
        }

        if (scriptReader == null)
            System.out.println("Type 'help' to get a list of available commands.");
        else {
            // Wait for Router announcement
            waitForRouters.waitHere();
        }

        mainLoop();

        close();
    }

    /**
     * Closed all resources created by CLI. This method will not close the <tt>QueueConnection</tt>!
     */
    public void close() {
        if (connectionHolder == null)
            return;
        connectionHolder.removeReconnectListener(this);
        endpointRegistry.close();
        if (!programmatic) {
            try {
                connectionHolder.close();
            } catch (Exception ignored) {
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println();
            System.out.println("usage: java com.swiftmq.admin.cli.CLI <smqp-url> <qcf> [<scriptfile>]");
            System.out.println();
            System.out.println("<smqp-url>   is the JNDI-Provider-URL like 'smqp://localhost:4001/timeout=10000'");
            System.out.println("<qcf>        is the name of the queue connection factory");
            System.out.println("             like 'QueueConnectionFactory'");
            System.out.println("<scriptfile> name of an optional file with CLI commands");
            System.out.println();
            System.out.println("See SwiftMQ's documentation for details");
            System.out.println();
            System.exit(-1);
        }

        try {
            CLI cli = new CLI(args[0], args[1], args.length == 3 ? args[2] : null);
            cli.run();
        } catch (Exception e) {
            System.out.println(e);
            System.exit(-1);
        }
    }
}

