package com.swiftmq.jms.springsupport;

import com.swiftmq.swiftlet.SwiftletManager;

public class SwiftMQLauncher {
    String workingDir = null;
    String configFile = null;
    boolean registerShutdownHook = false;

    public String getWorkingDir() {
        return workingDir;
    }

    public void setWorkingDir(String workingDir) {
        this.workingDir = workingDir;
    }

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public boolean isRegisterShutdownHook() {
        return registerShutdownHook;
    }

    public void setRegisterShutdownHook(boolean registerShutdownHook) {
        this.registerShutdownHook = registerShutdownHook;
    }

    public void startRouter() throws Exception {
        if (!registerShutdownHook)
            System.setProperty("swiftmq.shutdown.hook", "false");
        SwiftletManager.getInstance().setWorkingDirectory(workingDir);
        SwiftletManager.getInstance().startRouter(configFile);
    }

    public void shutdownRouter() {
        SwiftletManager.getInstance().shutdown();
    }
}
