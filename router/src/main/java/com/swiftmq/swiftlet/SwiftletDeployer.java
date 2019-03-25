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

import com.swiftmq.mgmt.XMLUtilities;
import com.swiftmq.swiftlet.deploy.DeploySpace;
import com.swiftmq.swiftlet.deploy.DeploySwiftlet;
import com.swiftmq.swiftlet.deploy.event.DeployListener;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.mgmt.CLIExecutor;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.deploy.Bundle;
import com.swiftmq.util.SwiftUtilities;

import java.util.List;

public class SwiftletDeployer implements DeployListener {
    static final String SPACE_NAME = "extension-swiftlets";

    DeploySwiftlet deploySwiftlet = null;
    DeploySpace deploySpace = null;
    MgmtSwiftlet mgmtSwiftlet = null;
    LogSwiftlet logSwiftlet = null;
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;

    SwiftletDeployer() {
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        SwiftletManager.getInstance().addSwiftletManagerListener("sys$log", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent evt) {
                logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
            }
        });
        SwiftletManager.getInstance().addSwiftletManagerListener("sys$mgmt", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent evt) {
                mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
            }
        });
        SwiftletManager.getInstance().addSwiftletManagerListener("sys$deploy", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent evt) {
                deploySwiftlet = (DeploySwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$deploy");
                deploySpace = deploySwiftlet.getDeploySpace(SPACE_NAME);
            }
        });
        SwiftletManager.getInstance().addSwiftletManagerListener("sys$scheduler", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent evt) {
                start();
            }
        });
        if (traceSpace.enabled) traceSpace.trace("SwiftletManager", toString() + "/created");
    }

    public void start() {
        if (traceSpace.enabled) traceSpace.trace("SwiftletManager", toString() + "/start ...");
        if (deploySpace != null) {
            // Load installed Extension Swiftlets
            try {
                Bundle[] bundles = deploySpace.getInstalledBundles();
                if (bundles != null) {
                    for (int i = 0; i < bundles.length; i++) {
                        if (traceSpace.enabled)
                            traceSpace.trace("SwiftletManager", toString() + "/start, installing bundle=" + bundles[i]);
                        try {
                            SwiftletManager.getInstance().loadExtensionSwiftlet(bundles[i]);
                        } catch (Exception e) {
                            e.printStackTrace();
                            if (traceSpace.enabled)
                                traceSpace.trace("SwiftletManager", toString() + "/start, bundle: " + bundles[i] + ", exception=" + e + ", trying to unload");
                            logSwiftlet.logError("SwiftletManager", toString() + "/start, bundle: " + bundles[i] + " exception=" + e + ", trying to unload");
                            try {
                                SwiftletManager.getInstance().unloadExtensionSwiftlet(bundles[i]);
                            } catch (Exception ignored) {
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (traceSpace.enabled)
                    traceSpace.trace("SwiftletManager", toString() + "/start, exception getting installed bundles: " + e);
            }

            // start listener
            deploySpace.setDeployListener(this);
        } else {
            logSwiftlet.logError("SwiftletManager", toString() + "/cannot deploy Extension Swiftlets, deploy space '" + SPACE_NAME + "' undefined!");
            if (traceSpace.enabled)
                traceSpace.trace("SwiftletManager", toString() + "/cannot deploy Extension Swiftlets, deploy space '" + SPACE_NAME + "' undefined!");
        }
        if (traceSpace.enabled) traceSpace.trace("SwiftletManager", toString() + "/start done");
    }

    private int executeCLICommands(Bundle bundle, CLIExecutor cliexec, String phase, boolean old) throws Exception {
        if (traceSpace.enabled)
            traceSpace.trace("SwiftletManager", toString() + "/executeCLICommands, bundle=" + bundle.getBundleName() + ", phase=" + phase + " ...");
        List cmdList = XMLUtilities.getCLICommands(old ? bundle.getOldBundleConfig() == null ? bundle.getBundleConfig() : bundle.getOldBundleConfig() : bundle.getBundleConfig(), phase);
        int ncmd = 0;
        if (cmdList == null || cmdList.size() == 0) {
            if (traceSpace.enabled)
                traceSpace.trace("SwiftletManager", toString() + "/executeCLICommands, no commands found.");
        } else {
            for (int i = 0; i < cmdList.size(); i++) {
                String cmd = (String) cmdList.get(i);
                if (cmd != null && cmd.trim().length() > 0) {
                    cmd = SwiftUtilities.substitute(cmd, "routername", SwiftletManager.getInstance().getRouterName());
                    if (traceSpace.enabled)
                        traceSpace.trace("SwiftletManager", toString() + "/executeCLICommands: " + cmd);
                    try {
                        cliexec.execute(cmd);
                    } catch (Exception e) {
                        if (!cmd.toLowerCase().startsWith("new "))
                            throw e;
                    }
                }
            }
            ncmd = cmdList.size();
        }
        if (traceSpace.enabled)
            traceSpace.trace("SwiftletManager", toString() + "/executeCLICommands, bundle=" + bundle.getBundleName() + ", phase=" + phase + " DONE.");
        return ncmd;
    }

    public void bundleAdded(Bundle bundle) throws Exception {
        if (traceSpace.enabled) traceSpace.trace("SwiftletManager", toString() + "/bundleAdded, bundle: " + bundle);
        CLIExecutor cliexec = mgmtSwiftlet.createCLIExecutor();
        int ncmd = 0;
        try {
            ncmd += executeCLICommands(bundle, cliexec, "before-install", false);
            SwiftletManager.getInstance().loadExtensionSwiftlet(bundle);
        } catch (Exception e) {
            SwiftletManager.getInstance().unloadExtensionSwiftlet(bundle);
            ncmd += executeCLICommands(bundle, cliexec, "after-remove", false);
            throw e;
        }
        if (ncmd > 0) {
            if (traceSpace.enabled)
                traceSpace.trace("SwiftletManager", toString() + "/bundleAdded, saving configuration");
            cliexec.execute("save");
        }
    }

    public void bundleRemoved(Bundle bundle, boolean isRedeploy) throws Exception {
        if (traceSpace.enabled) traceSpace.trace("SwiftletManager", toString() + "/bundleRemoved, bundle: " + bundle);
        CLIExecutor cliexec = mgmtSwiftlet.createCLIExecutor();
        int ncmd = 0;
        try {
            SwiftletManager.getInstance().unloadExtensionSwiftlet(bundle);
        } finally {
            ncmd += executeCLICommands(bundle, cliexec, "after-remove", false);
        }
        if (!isRedeploy) {
            if (ncmd > 0) {
                if (traceSpace.enabled)
                    traceSpace.trace("SwiftletManager", toString() + "/bundleRemoved, saving configuration");
                cliexec.execute("save");
            }
        }
    }

    public String toString() {
        return "SwiftletDeployer";
    }
}
