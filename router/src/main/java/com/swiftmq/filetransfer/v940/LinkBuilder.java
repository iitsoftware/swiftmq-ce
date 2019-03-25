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

package com.swiftmq.filetransfer.v940;

import com.swiftmq.filetransfer.Util;

public class LinkBuilder {
    String routerName = null;
    String cacheName = null;
    String fileKey = null;
    String digestType = null;
    byte[] digest = null;

    public LinkBuilder(String routerName, String cacheName, String fileKey, String digestType, byte[] digest) {
        this.routerName = routerName;
        this.cacheName = cacheName;
        this.fileKey = fileKey;
        this.digestType = digestType;
        this.digest = digest;
    }

    public LinkBuilder() {
    }

    public LinkBuilder routerName(String routerName) {
        this.routerName = routerName;
        return this;
    }

    public LinkBuilder cacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    public LinkBuilder fileKey(String fileKey) {
        this.fileKey = fileKey;
        return this;
    }

    public LinkBuilder digestType(String digestType) {
        this.digestType = digestType;
        return this;
    }

    public LinkBuilder digest(byte[] digest) {
        this.digest = digest;
        return this;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (routerName != null)
            sb.append("router=").append(routerName);
        if (cacheName != null) {
            if (sb.length() > 0)
                sb.append(';');
            sb.append("cache=").append(cacheName);
        }
        if (fileKey != null) {
            if (sb.length() > 0)
                sb.append(';');
            sb.append("file=").append(fileKey);
        }
        if (digest != null) {
            if (sb.length() > 0)
                sb.append(';');
            sb.append("digesttype=").append(digestType).append(';');
            sb.append("digest=").append(Util.byteArrayToHexString(digest));
        }
        return sb.toString();
    }
}
