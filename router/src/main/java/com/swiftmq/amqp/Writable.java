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

package com.swiftmq.amqp;

import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Interface to write an object to a DataOutput.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public interface Writable {
    /**
     * Returns an optional semaphore which is triggered after writeContent has been called. Can be used
     * to ensure an object has been written to a stream until processing continues.
     *
     * @return Semaphore
     */
    public Semaphore getSemaphore();

    /**
     * Alternatively returns a callback which is triggered after writeContent has been called. Can be used
     * to ensure an object has been written to a stream until processing continues.
     *
     * @return callback
     */
    public AsyncCompletionCallback getCallback();

    /**
     * Write the content of this object to a DataOutput
     *
     * @param out DataOutput
     * @throws IOException on error
     */
    public void writeContent(DataOutput out) throws IOException;
}
