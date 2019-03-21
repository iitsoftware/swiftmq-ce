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

package com.swiftmq.impl.jms.standard.v500;

import com.swiftmq.jms.smqp.v500.SMQPBulkRequest;
import com.swiftmq.jms.smqp.v500.SMQPFactory;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.InboundHandler;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestServiceRegistry;
import com.swiftmq.tools.util.DataStreamInputStream;

import java.io.IOException;
import java.io.InputStream;

public class InboundReader extends RequestServiceRegistry
    implements InboundHandler
{
  static final DumpableFactory dumpableFactory = new SMQPFactory();

  LogSwiftlet logSwiftlet = null;
  TraceSwiftlet traceSwiftlet = null;
  TraceSpace traceSpace = null;
  String tracePrefix;
  DataStreamInputStream dis = new DataStreamInputStream();

  InboundReader(String tracePrefix)
  {
    this.tracePrefix = tracePrefix;
    this.tracePrefix += "/InboundReader";
    logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
    traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
    traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_PROTOCOL);
  }

  public void dataAvailable(Connection connection, InputStream inputStream)
      throws IOException
  {
    dis.setInputStream(inputStream);
    Dumpable obj = Dumpalizer.construct(dis, dumpableFactory);
    if (traceSpace.enabled) traceSpace.trace("smqp", "read object: " + obj);
    if (obj.getDumpId() != SMQPFactory.DID_KEEP_ALIVE_REQ)
    {
      if (obj.getDumpId() == SMQPFactory.DID_BULK_REQ)
      {
        SMQPBulkRequest bulkRequest = (SMQPBulkRequest) obj;
        for (int i = 0; i < bulkRequest.len; i++)
        {
          Request req = (Request) bulkRequest.dumpables[i];
          if (req.getDumpId() != SMQPFactory.DID_KEEP_ALIVE_REQ)
            dispatch(req);
        }
      } else
        dispatch((Request) obj);
    }
  }
}

