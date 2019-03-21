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

package com.swiftmq.swiftlet.mgmt.event;

import java.util.EventListener;

/**
 * A MgmtListener is activated if an admin tool (SwiftMQ Explorer/CLI) is activated
 * or deactivated to inform other Swiftlet to, e.g., start resp. stop collectors.
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface MgmtListener extends EventListener
{

  /**
   * Will be called from the Mgmt Swiftlet when an admin tool is activated.
   */
  public void adminToolActivated();

  /**
   * Will be called from the Mgmt Swiftlet when an admin tool is deactivated.
   */
  public void adminToolDeactivated();
}

