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

public class Stack {
    private Object[] elements;
    private int size = 0;
    private int extendSize = 32;

    public Stack(int extendSize) {
        this.extendSize = extendSize;
        elements = new Object[extendSize];
    }

    public void push(Object obj) {
        if (size == elements.length) {
            Object[] newElements = new Object[elements.length + extendSize];
            System.arraycopy(elements, 0, newElements, 0, size);
            elements = newElements;
        }
        elements[size++] = obj;
    }

    public Object pop() {
        if (size == 0)
            return null;
        Object obj = elements[size - 1];
        elements[size - 1] = null;
        size--;
        return obj;
    }

    public int getSize() {
        return size;
    }

    public void clear() {
        for (int i = 0; i < elements.length; i++)
            elements[i] = null;
        size = 0;
    }
}
