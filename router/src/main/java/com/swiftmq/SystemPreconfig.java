/*
 * Copyright 2024 IIT Software GmbH
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

package com.swiftmq;

import java.io.File;
import java.util.Arrays;

public class SystemPreconfig {
    public static void main(String[] args) {
        File folder = new File("../data/preconfig");
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".xml"));

        if (files != null && files.length > 0) {
            Arrays.sort(files);

            StringBuilder preconfig = new StringBuilder();
            for (int i = 0; i < files.length; i++) {
                if (i > 0) {
                    preconfig.append(",");
                }
                preconfig.append(files[i].getPath());
            }

            System.out.println(preconfig);
        }
    }
}
