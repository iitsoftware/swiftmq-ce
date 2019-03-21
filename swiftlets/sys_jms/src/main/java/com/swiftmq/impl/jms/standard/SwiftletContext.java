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

package com.swiftmq.impl.jms.standard;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.AccountingSwiftlet;
import com.swiftmq.swiftlet.auth.AuthenticationSwiftlet;
import com.swiftmq.swiftlet.jndi.JNDISwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.net.NetworkSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class SwiftletContext
{
  public Configuration config = null;
  public Entity root = null;
  public EntityList usageList = null;
  public AuthenticationSwiftlet authSwiftlet = null;
  public AccountingSwiftlet accountingSwiftlet = null;
  public ThreadpoolSwiftlet threadpoolSwiftlet = null;
  public MgmtSwiftlet mgmtSwiftlet = null;
  public TimerSwiftlet timerSwiftlet = null;
  public NetworkSwiftlet networkSwiftlet = null;
  public QueueManager queueManager = null;
  public JNDISwiftlet jndiSwiftlet = null;
  public LogSwiftlet logSwiftlet = null;
  public TraceSwiftlet traceSwiftlet = null;
  public TraceSpace traceSpace = null;
  public JMSSwiftlet jmsSwiftlet = null;
  public boolean smartTree = false;
  public volatile int consumerCacheLowWaterMark = 0;

  public SwiftletContext(Configuration config, JMSSwiftlet jmsSwiftlet)
  {
    this.config = config;
    this.jmsSwiftlet = jmsSwiftlet;
    root = config;
    usageList = (EntityList) root.getEntity("usage");
    smartTree = SwiftletManager.getInstance().isUseSmartTree();
    if (smartTree)
      usageList.getTemplate().removeEntities();
    traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
    traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
    logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
    timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
    mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
    networkSwiftlet = (NetworkSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$net");
    queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
    jndiSwiftlet = (JNDISwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$jndi");
    authSwiftlet = (AuthenticationSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$authentication");
    accountingSwiftlet = (AccountingSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$accounting");
    threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
    Property prop = root.getProperty("consumer-cache-low-water-mark");
    consumerCacheLowWaterMark = ((Integer) prop.getValue()).intValue();
    prop.setPropertyChangeListener(new PropertyChangeListener()
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException
      {
        consumerCacheLowWaterMark = ((Integer) newValue).intValue();
      }
    });
  }
}
