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

package com.swiftmq.impl.deploy.standard;

import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.trace.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.mgmt.*;

public class SwiftletContext
{
  public ThreadpoolSwiftlet threadpoolSwiftlet = null;
  public LogSwiftlet logSwiftlet = null;
  public MgmtSwiftlet mgmtSwiftlet = null;
  public TimerSwiftlet timerSwiftlet = null;
  public TraceSwiftlet traceSwiftlet = null;
  public TraceSpace traceSpace = null;
  public DeploySwiftletImpl deploySwiftlet = null;
  public Configuration config = null;
  public EntityList usageList = null;
  public boolean isReboot = false;

  public SwiftletContext(DeploySwiftletImpl deploySwiftlet, Configuration config)
  {
    this.deploySwiftlet = deploySwiftlet;
    this.config = config;
    this.usageList = (EntityList)config.getEntity("usage");
    isReboot = SwiftletManager.getInstance().isRebooting();
    traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
    traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
    logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
    mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
    timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
    threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
  }
}
