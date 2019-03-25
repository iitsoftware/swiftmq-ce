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

package com.swiftmq.filetransfer;

/**
 * A progress listener can be set at the send or receive method of a Filetransfer object.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2013, All Rights Reserved
 */
public interface ProgressListener {
    /**
     * Will be called from the Filetransfer object and informs about the progress of the transfer.
     *
     * @param filename           name of the file
     * @param chunksTransferred  number of transferred chunks
     * @param fileSize           size of the file
     * @param bytesTransferred   number of bytes transferred
     * @param transferredPercent percent transferred
     */
    public void progress(String filename, int chunksTransferred, long fileSize, long bytesTransferred, int transferredPercent);
}
