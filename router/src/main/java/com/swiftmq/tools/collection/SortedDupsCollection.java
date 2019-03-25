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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedSet;

/**
 * A sorted collection that allowes duplicate entries.
 *
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class SortedDupsCollection extends AbstractCollection
        implements Iterator {
    SortedSet baseSet;
    int nElements = 0;
    Iterator baseIter = null;
    Entry lastIterEntry = null;
    int lastIterEntryPos = 0;

    /**
     * @param baseSet
     * @SBGen Constructor assigns baseSet
     */
    public SortedDupsCollection(SortedSet baseSet) {
        super();
        // SBgen: Assign variable
        this.baseSet = baseSet;
        nElements = baseSet.size();
    }

    /**
     * Ensures that this collection contains the specified element (optional
     * operation).  Returns <tt>true</tt> if the collection changed as a
     * result of the call.  (Returns <tt>false</tt> if this collection does
     * not permit duplicates and already contains the specified element.)
     * Collections that support this operation may place limitations on what
     * elements may be added to the collection.  In particular, some
     * collections will refuse to add <tt>null</tt> elements, and others will
     * impose restrictions on the type of elements that may be added.
     * Collection classes should clearly specify in their documentation any
     * restrictions on what elements may be added.<p>
     * <p>
     * This implementation always throws an
     * <tt>UnsupportedOperationException</tt>.
     *
     * @param o element whose presence in this collection is to be ensured.
     * @return <tt>true</tt> if the collection changed as a result of the call.
     * @throws UnsupportedOperationException if the <tt>add</tt> method is not
     *                                       supported by this collection.
     * @throws NullPointerException          if this collection does not permit
     *                                       <tt>null</tt> elements, and the specified element is
     *                                       <tt>null</tt>.
     * @throws ClassCastException            if the class of the specified element
     *                                       prevents it from being added to this collection.
     * @throws IllegalArgumentException      if some aspect of this element
     *                                       prevents it from being added to this collection.
     */
    public boolean add(Object o) {
        if (o == null)
            throw new NullPointerException();
        if (!(o instanceof Comparable))
            throw new ClassCastException("Object is not instance of Comparable!");
        Entry newEntry = new Entry((Comparable) o);
        Entry oldEntry = null;
        Iterator iter = baseSet.iterator();
        while (iter.hasNext()) {
            Entry entry = (Entry) iter.next();
            if (entry.equals(newEntry)) {
                oldEntry = entry;
                break;
            }
        }
        if (oldEntry != null) {
            oldEntry.add(newEntry.get(0));
            newEntry.removeAll();
        } else
            baseSet.add(newEntry);
        nElements++;
        return true;
    }

    public Object first() {
        Entry entry = (Entry) baseSet.first();
        if (entry != null)
            return entry.get(0);
        else
            return null;
    }

    /**
     * Returns an iterator over the elements contained in this collection.
     * *
     *
     * @return an iterator over the elements contained in this collection.
     */
    public Iterator iterator() {
        baseIter = baseSet.iterator();
        lastIterEntry = null;
        lastIterEntryPos = 0;
        return this;
    }

    /**
     * Returns the number of elements in this collection.  If the collection
     * contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     * *
     *
     * @return the number of elements in this collection.
     */
    public int size() {
        return nElements;
    }

    /**
     * Returns <tt>true</tt> if the iteration has more elements. (In other
     * words, returns <tt>true</tt> if <tt>next</tt> would return an element
     * rather than throwing an exception.)
     * *
     *
     * @return <tt>true</tt> if the iterator has more elements.
     */
    public boolean hasNext() {
        return baseIter.hasNext() || lastIterEntry != null && lastIterEntryPos < lastIterEntry.getSize();
    }

    /**
     * Returns the next element in the interation.
     * *
     *
     * @return the next element in the interation.
     */
    public Object next() {
        Object rObj = null;
        if (lastIterEntry == null)
            lastIterEntry = (Entry) baseIter.next();
        if (lastIterEntryPos < lastIterEntry.getSize())
            rObj = lastIterEntry.get(lastIterEntryPos++);
        else {
            lastIterEntry = (Entry) baseIter.next();
            lastIterEntryPos = 0;
            rObj = lastIterEntry.get(lastIterEntryPos++);
        }
        return rObj;
    }

    /**
     * *
     * Removes from the underlying collection the last element returned by the
     * iterator (optional operation).  This method can be called only once per
     * call to <tt>next</tt>.  The behavior of an iterator is unspecified if
     * the underlying collection is modified while the iteration is in
     * progress in any way other than by calling this method.
     * *
     */
    public void remove() {
        if (lastIterEntry != null) {
            lastIterEntry.remove(--lastIterEntryPos);
            if (lastIterEntry.getSize() == 0) {
                baseIter.remove();
                lastIterEntry = null;
                lastIterEntryPos = 0;
            }
            nElements--;
        }
    }

    private class Entry implements Comparable {
        ArrayList subEntries = new ArrayList();

        Entry(Comparable first) {
            subEntries.add(first);
        }

        void add(Comparable newSubEntry) {
            subEntries.add(newSubEntry);
        }

        int getSize() {
            return subEntries.size();
        }

        Comparable get(int index) {
            return (Comparable) subEntries.get(index);
        }

        void remove(int index) {
            subEntries.remove(index);
        }

        void removeAll() {
            subEntries.clear();
        }

        public boolean equals(Object that) {
            return compareTo(that) == 0;
        }

        public int compareTo(Object that) {
            return ((Comparable) subEntries.get(0)).compareTo(((Entry) that).get(0));
        }
    }
}

