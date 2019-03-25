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

import com.swiftmq.tools.gc.ObjectRecycler;

public class BucketTable {
    static final int DEFAULT_BUCKET_SIZE = 256;
    static final int DEFAULT_RECYCLE_SIZE = 2048;

    int bucketSize = 0;
    int recycleSize = 0;
    Bucket[] buckets = null;
    BucketRecycler recycler = null;
    int elements = 0;


    public BucketTable(int bucketSize, int recycleSize) {
        this.bucketSize = bucketSize;
        this.recycleSize = recycleSize;
        buckets = new Bucket[bucketSize];
        recycler = new BucketRecycler(recycleSize);
    }

    public BucketTable() {
        this(DEFAULT_BUCKET_SIZE, DEFAULT_RECYCLE_SIZE);
    }

    private Bucket getBucket(int bidx) {
        if (bidx >= buckets.length) {
            Bucket[] b = new Bucket[Math.max(buckets.length + bucketSize, bidx + bucketSize)];
            System.arraycopy(buckets, 0, b, 0, buckets.length);
            buckets = b;
        }
        if (buckets[bidx] == null)
            buckets[bidx] = (Bucket) recycler.checkOut();
        return buckets[bidx];
    }

    public void set(int index, Object obj) {
        int bidx = index / bucketSize;
        int eidx = index - (bidx * bucketSize);
        Bucket bucket = getBucket(bidx);
        bucket.array[eidx] = obj;
        bucket.count++;
        if (obj != null)
            elements++;
    }

    public Object get(int index) {
        int bidx = index / bucketSize;
        int eidx = index - (bidx * bucketSize);
        Bucket bucket = getBucket(bidx);
        return bucket.array[eidx];
    }

    public void remove(int index) {
        int bidx = index / bucketSize;
        int eidx = index - (bidx * bucketSize);
        Bucket bucket = getBucket(bidx);
        bucket.array[eidx] = null;
        bucket.count--;
        if (bucket.count == 0) {
            recycler.checkIn(bucket);
            buckets[bidx] = null;
        }
        elements--;
    }

    public int getElements() {
        return elements;
    }

    public int getSize() {
        return buckets.length * bucketSize;
    }

    public void clear() {
        recycler.clear();
        buckets = null;
    }

    private class Bucket {
        Object[] array = null;
        int count = 0;
    }

    private class BucketRecycler extends ObjectRecycler {
        public BucketRecycler(int maxSize) {
            super(maxSize);
        }

        protected Object createRecyclable() {
            Bucket bucket = new Bucket();
            bucket.array = new Object[bucketSize];
            return bucket;
        }
    }
}
