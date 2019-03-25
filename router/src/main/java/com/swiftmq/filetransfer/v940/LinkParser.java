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

public class LinkParser {
    String link = null;
    String routerName = null;
    String cacheName = null;
    String fileKey = null;
    String digestType = null;
    byte[] digest = null;

    public LinkParser(String link) {
        this.link = link;
        routerName = Util.parse("router=", ";", link);
        cacheName = Util.parse("cache=", ";", link);
        fileKey = Util.parse("file=", ";", link);
        digestType = Util.parse("digesttype=", ";", link);
        digest = Util.hexStringToByteArray(Util.parse("digest=", ";", link));
    }

    public String getLink() {
        return link;
    }

    public String getRouterName() {
        return routerName;
    }

    public String getCacheName() {
        return cacheName;
    }

    public String getFileKey() {
        return fileKey;
    }

    public String getDigestType() {
        return digestType;
    }

    public byte[] getDigest() {
        return digest;
    }
}
