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

package com.swiftmq.tools.file;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Comparator;

public class NumberBackupFileReducer {
    String directory;
    String prefix;
    int numberGenerations;

    public NumberBackupFileReducer(String directory, String prefix, int numberGenerations) {
        this.directory = directory;
        this.prefix = prefix;
        this.numberGenerations = numberGenerations;
    }

    private void sortByDateCreated(File[] files) {
        Arrays.sort(files, new Comparator<File>() {
            public int compare (File f1, File f2) {
                long l1 = getFileCreationEpoch(f1);
                long l2 = getFileCreationEpoch(f2);
                return Long.valueOf(l1).compareTo(l2);
            }
        });
    }

    private static long getFileCreationEpoch (File file) {
        try {
            BasicFileAttributes attr = Files.readAttributes(file.toPath(),
                    BasicFileAttributes.class);
            return attr.creationTime().toInstant().toEpochMilli();
        } catch (IOException e) {
            throw new RuntimeException(file.getAbsolutePath(), e);
        }
    }

    public void process() throws Exception {
        File[] files = new File(directory).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(prefix);
            }
        });
        if (files != null && files.length > numberGenerations) {
            sortByDateCreated(files);
            int toDelete = files.length-numberGenerations;
            for (int i=0;i<toDelete;i++){
                files[i].delete();
            }
        }
    }
}
