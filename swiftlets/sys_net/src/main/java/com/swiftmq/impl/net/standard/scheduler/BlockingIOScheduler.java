
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

import com.swiftmq.swiftlet.*;
import com.swiftmq.swiftlet.net.*;
import com.swiftmq.swiftlet.net.event.*;
import com.swiftmq.net.*;
import com.swiftmq.swiftlet.*;
import java.util.*;

public class BlockingIOScheduler extends IOScheduler
{	
	NetworkSwiftlet networkSwiftlet = null;
	
	/**
	 * @param metaData 
	 * @return 
	 */
	protected TCPListener createListenerInstance(ListenerMetaData metaData, SocketFactory socketFactory)
		throws Exception
	{
		if (networkSwiftlet == null)
			networkSwiftlet = (NetworkSwiftlet)SwiftletManager.getInstance().getSwiftlet("sys$net");
		
		if (networkSwiftlet.isReuseServerSocket())
		{
			Map serverSockets = (Map)SwiftletManager.getInstance().getSurviveData("sys$net/serversockets");
			if (serverSockets == null)
			{
				serverSockets = Collections.synchronizedMap(new HashMap());
				SwiftletManager.getInstance().addSurviveData("sys$net/serversockets",serverSockets);
			}
		}
		
		BlockingTCPListener l = new BlockingTCPListener(metaData, socketFactory); 
		// l.start();
		return l;
	}
	
	/**
	 * @param metaData 
	 * @return 
	 */
	protected synchronized TCPConnector createConnectorInstance(ConnectorMetaData metaData, SocketFactory socketFactory)
		throws Exception
	{
		BlockingTCPConnector c = new BlockingTCPConnector(metaData,socketFactory);
		// c.start();
		return c;
	}
	
	public String toString()
	{
		return "BlockingIOScheduler";
	}
}
