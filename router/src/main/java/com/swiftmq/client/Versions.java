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

package com.swiftmq.client;

public class Versions {
    public static final int[] JNDI = {400};
    public static final int JNDI_CURRENT = 400;
    public static final int[] JMS = {400, 500, 510, 600, 610, 630, 750};
    public static int JMS_CURRENT = Integer.parseInt(System.getProperty("swiftmq.smqp.version", "750"));

    public static int getSelectedIndex(int current, int[] set) {
        for (int i = 0; i < set.length; i++) {
            if (set[i] == current)
                return i;
        }
        return set.length - 1;
    }

    public static int[] cutAfterIndex(int idx, int[] set) {
        int[] newSet = new int[idx + 1];
        for (int i = 0; i < newSet.length; i++)
            newSet[i] = set[i];
        return newSet;
    }
}
