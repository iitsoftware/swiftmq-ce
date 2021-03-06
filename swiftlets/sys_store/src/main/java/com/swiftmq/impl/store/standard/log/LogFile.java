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

import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;

public interface LogFile {
    public void init(long maxSize);

    public void setInMemoryMode(boolean b);

    public boolean isInMemoryMode();

    public RandomAccessFile getFile();

    public void write(LogRecord logRecord) throws IOException;

    public void write(LogRecord logRecord, DataOutput copyHere) throws IOException;

    public int getFlushSize();

    public long getPosition();

    public void flush(boolean sync) throws IOException;

    public void reset(boolean sync) throws IOException;

    public void reset(boolean sync, DataOutput copyHere) throws IOException;
}
