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

package com.swiftmq.tools.collection;

import java.util.ArrayList;

/**
 * A tool class for ArrayLists
 *
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class ArrayListTool {
    /**
     * set the object in the list on the first position with value equals null
     * or - if no free index exists - expand.
     *
     * @param list   the list
     * @param object the object to insert
     * @return the index for this object
     */
    public static int setFirstFreeOrExpand(ArrayList list, Object object) {
        int idx = list.indexOf(null);
        if (idx == -1) {
            idx = list.size();
            list.add(object);
        } else
            list.set(idx, object);
        return idx;
    }
}
