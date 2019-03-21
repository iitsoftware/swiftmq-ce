
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

package com.swiftmq.impl.net.standard.scheduler;

import com.swiftmq.swiftlet.net.event.*;
import com.swiftmq.net.*;
import java.util.*;
import java.io.*;
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.net.ListenerMetaData;

public abstract class TCPListener
{
  ListenerMetaData metaData;

  /**
   * @param metaData 
   * @SBGen Constructor assigns metaData
   */
  public TCPListener(ListenerMetaData metaData)
  {
    // SBgen: Assign variable
    this.metaData = metaData;
  }

  /**
   * @return 
   * @SBGen Method get metaData
   */
  public ListenerMetaData getMetaData()
  {
    // SBgen: Get variable
    return(metaData);
  }

  /**
   * @exception IOException 
   */
  public abstract void start()
    throws IOException;

  public abstract void close();
	
	public String toString()
	{
		StringBuffer b = new StringBuffer();
		b.append("swiftlet=");
		b.append(metaData.getSwiftlet().getName());
		b.append(", port=");
		b.append(metaData.getPort());
		return b.toString();
	}
}

