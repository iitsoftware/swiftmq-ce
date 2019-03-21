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

package com.swiftmq.impl.streams;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;

/**
 * Convenience class that provides methods to access environment properties. It is accessible from
 * Streams scripts via variable "os".
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class OsSupport {
    FileSystem fileSystem;
    MBeanServer mbs;
    ObjectName name;

    OsSupport() {
        this.fileSystem = FileSystems.getDefault();
        this.mbs = ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * Returns the total space of a partition referenced by a path.
     *
     * @param path path
     * @return total space
     * @throws Exception
     */
    public long totalSpace(String path) throws Exception {
        FileStore fileStore = Files.getFileStore(fileSystem.getPath(path));
        return fileStore.getTotalSpace();
    }

    /**
     * Returns the usable space of a partition referenced by a path.
     *
     * @param path path
     * @return usable space
     * @throws Exception
     */
    public long usableSpace(String path) throws Exception {
        FileStore fileStore = Files.getFileStore(fileSystem.getPath(path));
        return fileStore.getUsableSpace();
    }

    /**
     * Returns the unallocated (free) space of a partition referenced by a path.
     *
     * @param path path
     * @return usable space
     * @throws Exception
     */
    public long unallocatedSpace(String path) throws Exception {
        FileStore fileStore = Files.getFileStore(fileSystem.getPath(path));
        return fileStore.getUnallocatedSpace();
    }

    /**
     * Returns the current CPU load of the Router process.
     *
     * @return cpu load in percent
     * @throws Exception
     */
    public int processCpuLoad() throws Exception {

        ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
        AttributeList list = mbs.getAttributes(name, new String[]{"ProcessCpuLoad"});

        if (list.isEmpty()) return 0;

        Attribute att = (Attribute) list.get(0);
        Double value = (Double) att.getValue();

        // usually takes a couple of seconds before we get real values
        if (value == -1.0) return 0;
        // returns a percentage value with 1 decimal point precision
        return (int) ((value * 1000) / 10.0);
    }
}
