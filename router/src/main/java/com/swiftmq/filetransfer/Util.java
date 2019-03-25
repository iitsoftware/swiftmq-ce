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

package com.swiftmq.filetransfer;

public class Util {
    public static String parse(String pre, String post, String source) {
        int start = source.indexOf(pre);
        if (start == -1)
            return null;
        int valStart = start + pre.length();
        int valStop = source.length();
        int end = source.indexOf(post, start);
        if (end != -1)
            valStop = end;
        return source.substring(valStart, valStop);
    }

    public static String byteArrayToHexString(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < b.length; i++)
            sb.append(String.format("%02x", b[i]));
        return sb.toString();
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] b = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            b[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return b;
    }

    public static String createCacheRequestQueueName(String cacheName, String routerName) {
        return new StringBuilder("swiftmqfcreq-").append(cacheName).append('@').append(routerName).toString();
    }
}