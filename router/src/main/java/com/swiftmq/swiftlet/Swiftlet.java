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

import com.swiftmq.mgmt.Configuration;

/**
 * Swiftlets are components of a SwiftMQ router and are under control of the SwiftletManager.
 * <br><br>
 * Implementing classes overwrite the <code>startup</code> and <code>shutdown</code> method.
 * <br><br>
 * A Swiftlet is instantiated from the SwiftletManager. All Kernel Swiftlets are sharing one
 * class loader, and every Extension Swiftlet has his own class loader.
 * @see SwiftletManager
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class Swiftlet
{
  public final static int STATE_ACTIVE = 0;
  public final static int STATE_INACTIVE = 1;
  public final static int STATE_STANDBY  = 2;

  String name;
  int state = STATE_INACTIVE;
  long startupTime;
  boolean kernel = false;
  Configuration __myconf = null;

  void setName(String name)
  {
    this.name = name;
  }

  /**
   * Returns the name of the Swiftlet
   * @return swiftlet name
   */
  public String getName()
  {
    return (name);
  }

  /**
   * Set a new swiftlet state
   */
  void setState(int state)
  {
    this.state = state;
  }

  /**
   * Returns the current swiftlet state
   * @return current state
   */
  public int getState()
  {
    return state;
  }

  void setKernel(boolean b)
  {
    this.kernel = b;
  }

  boolean isKernel()
  {
    return kernel;
  }

  /**
   * Set the startup time.
   * Called from the SwiftletManager.
   * @param startupTime startup time.
   */
  protected void setStartupTime(long startupTime)
  {
    this.startupTime = startupTime;
  }

  /**
   * Returns the Swiftlet startup time
   * @return startup time.
   */
  public long getStartupTime()
  {
    return (startupTime);
  }

  /**
   * Returns whether this Swiftlet provides Snapshots
   * @return snapshot available.
   */
  public boolean isSnapshotAvailable()
  {
    return false;
  }

  /**
   * Start this Swiftlet.
   * Called from the SwiftletManager during router start. The Swiftlet configuration is
   * passed as parameter.
   * @param config Swiftlet configuration.
   * @exception SwiftletException on error during startup.
   */
  protected abstract void startup(Configuration config) throws SwiftletException;

  /**
   * Start this Swiftlet in standby mode. This method must be implemented from
   * Kernel Swiftlets only.
   * @param config Swiftlet configuration.
   * @exception SwiftletException on error during startup.
   */
  protected void standby(Configuration config) throws SwiftletException
  {
    __myconf = config;
  }

  /**
   * Resume this Swiftlet after it has been in standby mode. The Swiftlet changes to active.
   * This method must be implemented from Kernel Swiftlets only.
   * @exception SwiftletException on error during startup.
   */
  protected void resume() throws SwiftletException
  {
    startup(__myconf);
  }


  /**
   * Stop this Swiftlet.
   * Called from the SwiftletManager during router shutdown.
   * @exception SwiftletException on error during shutdown.
   * @see package.class
   */
  protected abstract void shutdown() throws SwiftletException;
}

