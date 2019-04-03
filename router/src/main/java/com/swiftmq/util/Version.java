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

package com.swiftmq.util;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Version {
    private static String VERSION_PROP = System.getProperty("swiftmq.version.properties", "../version.properties");
    private static Properties props = new Properties();
    static {
        try {
            props.load(new FileReader(VERSION_PROP));
        } catch (IOException e) {
            System.err.println("Unable to load version properties from file: "+VERSION_PROP);
            System.exit(-1);
        }
    }
    public static String getKernelVendor() {
        return props.getProperty("vendor");
    }

    public static String getKernelVersion() {
        return props.getProperty("release")+" "+props.getProperty("distribution");
    }

    public static String getKernelConfigRelease() {
        return props.getProperty("release");
    }

    public static int getSwiftmqMajorVersion() {
        return Integer.parseInt(props.getProperty("release").substring(0, 2));
    }

    public static int getSwiftmqMinorVersion() {
        return Integer.parseInt(props.getProperty("release").substring(3, 5));
    }
}