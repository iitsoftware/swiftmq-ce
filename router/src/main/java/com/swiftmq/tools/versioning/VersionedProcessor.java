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

package com.swiftmq.tools.versioning;

import com.swiftmq.tools.collection.RingBuffer;
import com.swiftmq.tools.versioning.event.VersionedListener;

public class VersionedProcessor {
    int[] acceptedVersions = null;
    VersionedListener listener = null;
    VersionedConverter converter = null;
    VersionedFactory factory = null;
    RingBuffer buffer = null;

    public VersionedProcessor(int[] acceptedVersions, VersionedListener listener, VersionedConverter converter, VersionedFactory factory, boolean useBuffer) {
        this.acceptedVersions = acceptedVersions;
        this.listener = listener;
        this.converter = converter;
        this.factory = factory;
        if (useBuffer && listener == null)
            buffer = new RingBuffer(32);
    }

    public void setListener(int[] acceptedVersions, VersionedListener listener) {
        this.acceptedVersions = acceptedVersions;
        this.listener = listener;
    }

    public void setConverter(VersionedConverter converter) {
        this.converter = converter;
    }

    public void setFactory(VersionedFactory factory) {
        this.factory = factory;
    }

    protected boolean isReady() {
        return true;
    }

    public void processBuffer() {
        if (buffer == null)
            return;
        int size = buffer.getSize();
        for (int i = 0; i < size; i++)
            process((VersionedDumpable) buffer.remove());
    }

    public void process(Versioned versioned) {
        try {
            process(new VersionedDumpable(versioned.getVersion(), factory.createDumpable(versioned)));
        } catch (VersionedException e) {
            listener.onException(e);
        }
    }

    public void process(VersionedDumpable vd) {
        if (listener == null || !isReady()) {
            if (buffer != null)
                buffer.add(vd);
            return;
        }
        try {
            boolean found = false;
            for (int i = 0; i < acceptedVersions.length; i++) {
                if (vd.getVersion() == acceptedVersions[i]) {
                    found = true;
                    break;
                }
            }
            if (!found)
                converter.convert(vd, acceptedVersions);
            listener.onAccept(vd);
        } catch (VersionedException e) {
            listener.onException(e);
        }
    }
}
