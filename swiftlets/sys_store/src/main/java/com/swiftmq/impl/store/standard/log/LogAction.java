
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

package com.swiftmq.impl.store.standard.log;

import java.io.*;

public abstract class LogAction
{
	public final static int INSERT = 1;
	public final static int UPDATE = 2;
	public final static int DELETE = 3;
	public final static int UPDATE_PORTION = 4;
	
	public static LogAction create(int type)
	{
		LogAction la = null;
		switch (type)
		{
		case INSERT:
			la = new InsertLogAction(0);
			break;
		case UPDATE:
			la = new UpdateLogAction(null,null);
			break;
		case DELETE:
			la = new DeleteLogAction(0,null);
			break;
		case UPDATE_PORTION:
			la = new UpdatePortionLogAction(0,0,null,null);
			break;
		}
		return la;
	}
	
	public abstract int getType();
	
	/**
	 * @param out 
	 * @exception IOException 
	 */
	protected abstract void writeContent(DataOutput out)
		throws IOException;
	
	/**
	 * @param in 
	 * @exception IOException 
	 */
	protected abstract void readContent(DataInput in)
		throws IOException;
}

