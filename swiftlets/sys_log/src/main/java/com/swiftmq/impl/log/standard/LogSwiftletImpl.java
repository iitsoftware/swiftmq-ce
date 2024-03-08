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

package com.swiftmq.impl.log.standard;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.log.LogSink;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.tools.file.NumberGenerationProvider;
import com.swiftmq.tools.file.RollingFileWriter;
import com.swiftmq.tools.file.RolloverSizeProvider;
import com.swiftmq.util.SwiftUtilities;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LogSwiftletImpl extends LogSwiftlet {
    static SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S/");

    Configuration config = null;
    PrintWriter infoWriter = null;
    String infoFileName = null;
    PrintWriter warningWriter = null;
    String warningFileName = null;
    PrintWriter errorWriter = null;
    String errorFileName = null;
    int maxSize = 0;
    final AtomicBoolean infoEnabled = new AtomicBoolean(true);
    final AtomicBoolean warningEnabled = new AtomicBoolean(true);
    final AtomicBoolean errorEnabled = new AtomicBoolean(true);
    RolloverSizeProvider rolloverSizeProvider = null;
    NumberGenerationProvider numberGenerationProvider = null;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private String createLogLine(String source, String type, String msg) {
        Calendar cal = Calendar.getInstance();
        StringBuffer outline = new StringBuffer();
        outline.append(fmt.format(cal.getTime()));
        outline.append(source);
        outline.append("/");
        outline.append(type);
        outline.append("/");
        outline.append(msg);
        return outline.toString();
    }

    /**
     * Log an information message
     *
     * @param source  usually the swiftlet name
     * @param message the message to log
     */
    public void logInformation(String source, String message) {
        lock.readLock().lock();
        try {
            if (!infoEnabled.get())
                return;
            infoWriter.println(createLogLine(source, "INFORMATION", message));
        } finally {
            lock.readLock().unlock();
        }

    }

    /**
     * Log a warning message
     *
     * @param source  usually the swiftlet name
     * @param message the message to log
     */
    public void logWarning(String source, String message) {
        lock.readLock().lock();
        try {
            if (!warningEnabled.get())
                return;
            warningWriter.println(createLogLine(source, "WARNING", message));
        } finally {
            lock.readLock().unlock();
        }

    }

    /**
     * Log an error message
     *
     * @param source  usually the swiftlet name
     * @param message the message to log
     */
    public void logError(String source, String message) {
        lock.readLock().lock();
        try {
            if (!errorEnabled.get())
                return;
            errorWriter.println(createLogLine(source, "ERROR", message));
        } finally {
            lock.readLock().unlock();
        }

    }

    public LogSink createLogSink(String s) {
        Property prop = config.getProperty("logsink-directory");
        String dirName = SwiftUtilities.addWorkingDir((String) prop.getValue());
        String filename = dirName + File.separatorChar + s + ".log";
        new File(dirName).mkdirs();
        try {
            return new LogSinkImpl(new PrintWriter(new RollingFileWriter(filename, rolloverSizeProvider, numberGenerationProvider), true));
        } catch (IOException e) {
            logError("sys$log", "Unable to create log sink directory " + filename + ": " + e.toString());
            return null;
        }
    }

    private void createLogFiles(final Entity root) throws Exception {
        rolloverSizeProvider = new RolloverSizeProvider() {
            public long getRollOverSize() {
                return maxSize * 1024L;
            }
        };
        numberGenerationProvider = new NumberGenerationProvider() {
            public int getNumberGenerations() {
                return (Integer) root.getProperty("number-old-logfile-generations").getValue();
            }
        };
        Property prop = root.getProperty("size-limit");
        maxSize = (Integer) prop.getValue();

        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                maxSize = (Integer) newValue;
            }
        });

        prop = root.getProperty("logfile-info");
        String infoFile = SwiftUtilities.addWorkingDir((String) prop.getValue());
        new File(infoFile).getParentFile().mkdirs();
        infoFileName = infoFile;
        infoWriter = new PrintWriter(new RollingFileWriter(infoFile, rolloverSizeProvider, numberGenerationProvider), true);

        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                if (newValue == null)
                    throw new PropertyChangeException("Null Value not allowed.");
                lock.writeLock().lock();
                try {
                    try {
                        String s = SwiftUtilities.addWorkingDir((String) newValue);
                        new File(s).getParentFile().mkdirs();
                        PrintWriter pw = new PrintWriter(new RollingFileWriter(s, rolloverSizeProvider, numberGenerationProvider), true);
                        if (infoWriter != warningWriter && infoWriter != errorWriter)
                            infoWriter.close();
                        infoWriter = pw;
                        infoFileName = s;
                    } catch (Exception e) {
                        throw new PropertyChangeException(e.getMessage());
                    }
                } finally {
                    lock.writeLock().unlock();
                }

            }
        });

        prop = root.getProperty("logfile-warning");
        String warningFile = SwiftUtilities.addWorkingDir((String) prop.getValue());
        warningFileName = warningFile;
        new File(warningFileName).getParentFile().mkdirs();
        if (warningFile.equals(infoFile))
            warningWriter = infoWriter;
        else
            warningWriter = new PrintWriter(new RollingFileWriter(warningFile, rolloverSizeProvider, numberGenerationProvider), true);

        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                if (newValue == null)
                    throw new PropertyChangeException("Null Value not allowed.");
                lock.writeLock().lock();
                try {
                    try {
                        String s = SwiftUtilities.addWorkingDir((String) newValue);
                        new File(s).getParentFile().mkdirs();
                        PrintWriter pw = new PrintWriter(new RollingFileWriter(s, rolloverSizeProvider, numberGenerationProvider), true);
                        if (warningWriter != infoWriter && warningWriter != errorWriter)
                            warningWriter.close();
                        warningWriter = pw;
                        warningFileName = s;
                    } catch (Exception e) {
                        throw new PropertyChangeException(e.getMessage());
                    }
                } finally {
                    lock.writeLock().unlock();
                }

            }
        });

        prop = root.getProperty("logfile-error");
        String errorFile = SwiftUtilities.addWorkingDir((String) prop.getValue());
        errorFileName = errorFile;
        new File(errorFileName).getParentFile().mkdirs();
        if (errorFile.equals(infoFile))
            errorWriter = infoWriter;
        else if (errorFile.equals(warningFile))
            errorWriter = warningWriter;
        else
            errorWriter = new PrintWriter(new RollingFileWriter(errorFile, rolloverSizeProvider, numberGenerationProvider), true);

        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                if (newValue == null)
                    throw new PropertyChangeException("Null Value not allowed.");
                lock.writeLock().lock();
                try {
                    try {
                        String s = SwiftUtilities.addWorkingDir((String) newValue);
                        new File(s).getParentFile().mkdirs();
                        PrintWriter pw = new PrintWriter(new RollingFileWriter(s, rolloverSizeProvider, numberGenerationProvider), true);
                        if (errorWriter != infoWriter && errorWriter != warningWriter)
                            errorWriter.close();
                        errorWriter = pw;
                        errorFileName = s;
                    } catch (Exception e) {
                        throw new PropertyChangeException(e.getMessage());
                    }
                } finally {
                    lock.writeLock().unlock();
                }

            }
        });

        prop = root.getProperty("logfile-info-enabled");
        infoEnabled.set(((Boolean) prop.getValue()).booleanValue());
        if (!infoEnabled.get())
            infoWriter.println(createLogLine(getName(), "CONFIGURATION", "Info Logfile disabled"));
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                infoEnabled.set((Boolean) newValue);
                if (!infoEnabled.get())
                    infoWriter.println(createLogLine(getName(), "CONFIGURATION", "Info Logfile disabled"));
            }
        });

        prop = root.getProperty("logfile-warning-enabled");
        warningEnabled.set((Boolean) prop.getValue());
        if (!warningEnabled.get())
            warningWriter.println(createLogLine(getName(), "CONFIGURATION", "Warning Logfile disabled"));
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                warningEnabled.set((Boolean) newValue);
                if (!warningEnabled.get())
                    warningWriter.println(createLogLine(getName(), "CONFIGURATION", "Warning Logfile disabled"));
            }
        });

        prop = root.getProperty("logfile-error-enabled");
        errorEnabled.set((Boolean) prop.getValue());
        if (!errorEnabled.get())
            errorWriter.println(createLogLine(getName(), "CONFIGURATION", "Error Logfile disabled"));
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                errorEnabled.set((Boolean) newValue);
                if (!errorEnabled.get())
                    errorWriter.println(createLogLine(getName(), "CONFIGURATION", "Error Logfile disabled"));
            }
        });
    }

    /**
     * Startup the swiftlet. Check if all required properties are defined and all other
     * startup conditions are met. Do startup work (i. e. start working thread, get/open resources).
     * If any condition prevends from startup fire a SwiftletException.
     *
     * @throws com.swiftmq.swiftlet.SwiftletException
     */
    protected void startup(Configuration config)
            throws SwiftletException {
        this.config = config;
        try {
            createLogFiles(config);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
    }

    /**
     * Shutdown the swiftlet. Check if all shutdown conditions are met. Do shutdown work (i. e. stop working thread, close resources).
     * If any condition prevends from shutdown fire a SwiftletException.
     *
     * @throws com.swiftmq.swiftlet.SwiftletException
     */
    protected void shutdown()
            throws SwiftletException {
        try {
            infoWriter.close();
        } catch (Exception ignored) {
        }
        try {
            if (warningWriter != infoWriter)
                warningWriter.close();
        } catch (Exception ignored) {
        }
        try {
            if (errorWriter != infoWriter && errorWriter != warningWriter)
                errorWriter.close();
        } catch (Exception ignored) {
        }
    }
}

