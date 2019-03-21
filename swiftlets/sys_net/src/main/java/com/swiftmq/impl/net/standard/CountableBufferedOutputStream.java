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

package com.swiftmq.impl.net.standard;

import com.swiftmq.net.protocol.*;

import java.io.*;

public class CountableBufferedOutputStream extends OutputStream
  implements Countable, OutputListener
{
  ProtocolOutputHandler protocolOutputHandler = null;
  OutputStream out = null;
  volatile long byteCount = 0;

  public CountableBufferedOutputStream(ProtocolOutputHandler protocolOutputHandler, OutputStream out)
  {
    this.protocolOutputHandler = protocolOutputHandler;
    this.out = out;
    protocolOutputHandler.setOutputListener(this);
  }

  public void write(byte[] b, int offset, int len) throws IOException
  {
    byteCount += len;
    protocolOutputHandler.write(b, offset, len);
  }

  public void write(int b) throws IOException
  {
    byteCount++;
    protocolOutputHandler.write(b);
  }

  public void flush() throws IOException
  {
    protocolOutputHandler.flush();
    protocolOutputHandler.invokeOutputListener();
  }

  public int performWrite(byte[] b, int offset, int len)
    throws IOException
  {
    out.write(b, offset, len);
    out.flush();
    return len;
  }

  public void addByteCount(long cnt)
  {
    byteCount += cnt;
  }

  public long getByteCount()
  {
    return byteCount;
  }

  public void resetByteCount()
  {
    byteCount = 0;
  }
}

