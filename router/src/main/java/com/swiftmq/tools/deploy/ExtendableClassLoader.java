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

package com.swiftmq.tools.deploy;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

public class ExtendableClassLoader extends URLClassLoader {
    File root = null;
    long created = 0;

    public ExtendableClassLoader(File root, URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.root = root;
        created = System.currentTimeMillis();
    }

    public void add(URL url) {
        addURL(url);
    }

    public void add(URL[] urls) {
        for (int i = 0; i < urls.length; i++)
            addURL(urls[i]);
    }

    Class _findClass(String name) throws ClassNotFoundException {
        return findClass(name);
    }

    protected String findLibrary(String libname) {
        String lib = System.mapLibraryName(libname);
        File[] files = root.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    File[] df = files[i].listFiles();
                    for (int j = 0; j < df.length; j++) {
                        if (df[j].getName().equals(lib))
                            return df[j].getAbsolutePath();
                    }
                } else if (files[i].getName().equals(lib))
                    return files[i].getAbsolutePath();
            }
        }
        return null;
    }

    public String toString() {
        StringBuffer b = new StringBuffer();
        URL[] urls = getURLs();
        b.append("[ExtendableClassLoader, created=" + created + ", urls=");
        for (int i = 0; i < urls.length; i++) {
            if (i > 0)
                b.append(",");
            b.append(urls[i].toString());
        }
        b.append("]");
        return b.toString();
    }
}

