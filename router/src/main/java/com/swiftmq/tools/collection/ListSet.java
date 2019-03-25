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

import java.util.*;

public class ListSet {
    List list = new LinkedList();
    Set set = new HashSet();
    int max = 0;

    public ListSet(int max) {
        this.max = max;
    }

    public void resize(int newSize) {
        while (list.size() > 0 && list.size() > newSize) {
            Object first = list.remove(0);
            set.remove(first);
        }
        max = newSize;
    }

    public void add(Object o) {
        if (set.contains(o))
            return;
        if (list.size() == max) {
            Object first = list.remove(0);
            set.remove(first);
        }
        list.add(o);
        set.add(o);
    }

    public ListIterator forwardIterator() {
        return list.listIterator();
    }

    public ListIterator backwardIterator() {
        return list.listIterator(list.size());
    }

    public void addAll(Collection c) {
        for (Iterator iter = c.iterator(); iter.hasNext(); )
            add(iter.next());
    }

    public void removeAll(Collection c) {
        for (Iterator iter = c.iterator(); iter.hasNext(); )
            remove(iter.next());
    }

    public boolean remove(Object o) {
        list.remove(o);
        return set.remove(o);
    }

    public boolean contains(Object o) {
        return set.contains(o);
    }

    public int size() {
        return set.size();
    }

    public int getMax() {
        return max;
    }

    public void clear() {
        list.clear();
        set.clear();
    }
}
