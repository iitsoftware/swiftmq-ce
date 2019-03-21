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

package com.swiftmq.tools.deploy;

import java.net.URLClassLoader;
import java.util.List;

public class Bundle
{
  String bundleName;
  String bundleDir;
  URLClassLoader bundleLoader;
  List bundleJars = null;
  String bundleConfig;
  String oldBundleConfig;
  long changeTimeConfig = -1;
  int event = BundleEvent.BUNDLE_UNCHANGED;

  /**
   * @param bundleName
   * @SBGen Constructor assigns bundleName
   */
  Bundle(String bundleName)
  {
    // SBgen: Assign variables
    this.bundleName = bundleName;
    // SBgen: End assign
  }


  /**
   * @param event
   * @SBGen Method set event
   */
  void setEvent(int event)
  {
    // SBgen: Assign variable
    this.event = event;
  }

  /**
   * @return
   * @SBGen Method get event
   */
  int getEvent()
  {
    // SBgen: Get variable
    return (event);
  }

  /**
   * @return
   * @SBGen Method get bundleName
   */
  public String getBundleName()
  {
    // SBgen: Get variable
    return (bundleName);
  }

  /**
   * @param bundleDir
   * @SBGen Method set bundleDir
   */
  void setBundleDir(String bundleDir)
  {
    // SBgen: Assign variable
    this.bundleDir = bundleDir;
  }

  /**
   * @return
   * @SBGen Method get bundleDir
   */
  public String getBundleDir()
  {
    // SBgen: Get variable
    return (bundleDir);
  }

  /**
   * @param bundleLoader
   * @SBGen Method set bundleLoader
   */
  void setBundleLoader(URLClassLoader bundleLoader)
  {
    // SBgen: Assign variable
    this.bundleLoader = bundleLoader;
  }

  /**
   * @return
   * @SBGen Method get bundleLoader
   */
  public URLClassLoader getBundleLoader()
  {
    // SBgen: Get variable
    return (bundleLoader);
  }

  /**
   * @param bundleJars
   * @SBGen Method set bundleJars
   */
  void setBundleJars(List bundleJars)
  {
    // SBgen: Assign variable
    this.bundleJars = bundleJars;
  }

  /**
   * @return
   * @SBGen Method get bundleJars
   */
  List getBundleJars()
  {
    // SBgen: Get variable
    return (bundleJars);
  }

  /**
   * @param bundleConfig
   * @param changeTimeConfig
   * @SBGen Method set bundleConfig
   */
  void setBundleConfig(String bundleConfig, long changeTimeConfig)
  {
    // SBgen: Assign variable
    oldBundleConfig = this.bundleConfig;
    this.bundleConfig = bundleConfig;
    this.changeTimeConfig = changeTimeConfig;
  }

  /**
   * @return
   * @SBGen Method get bundleConfig
   */
  public String getBundleConfig()
  {
    // SBgen: Get variable
    return (bundleConfig);
  }

  /**
   * @return
   * @SBGen Method get oldBundleConfig
   */
  public String getOldBundleConfig()
  {
    // SBgen: Get variable
    return (oldBundleConfig);
  }

  /**
   * @return
   * @SBGen Method get changeTimeConfig
   */
  long getChangeTimeConfig()
  {
    // SBgen: Get variable
    return (changeTimeConfig);
  }

  public String toString()
  {
    StringBuffer b = new StringBuffer("[Bundle, bundleName=");
    b.append(bundleName);
    b.append(", dir=");
    b.append(bundleDir);
    b.append(", event=");
    switch (event)
    {
      case BundleEvent.BUNDLE_UNCHANGED:
        b.append("BUNDLE_UNCHANGED");
        break;
      case BundleEvent.BUNDLE_ADDED:
        b.append("BUNDLE_ADDED");
        break;
      case BundleEvent.BUNDLE_REMOVED:
        b.append("BUNDLE_REMOVED");
        break;
      case BundleEvent.BUNDLE_CHANGED:
        b.append("BUNDLE_CHANGED");
        break;
    }
    b.append(", oldBundleConfig=");
    b.append(oldBundleConfig == null?"null":"too large to display");
    b.append(", bundleConfig=");
    b.append(bundleConfig == null?"null":"too large to display");
    b.append(", changeTimeConfig=");
    b.append(changeTimeConfig);
    b.append(", bundleLoader=");
    if (bundleLoader == null)
      b.append("null");
    else
      b.append(bundleLoader.toString());
    b.append(", bundleJars=");
    b.append(bundleJars);
    b.append("]");
    return b.toString();
  }
}

