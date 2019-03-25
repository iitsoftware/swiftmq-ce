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

package com.swiftmq.auth;

import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.IOException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.util.Arrays;

public class ChallengeResponseFactoryImpl implements ChallengeResponseFactory {
    public Serializable createChallenge(String password) {
        return new Long(System.currentTimeMillis());
    }

    public byte[] createBytesChallenge(String password) {
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        try {
            dbos.writeLong(System.currentTimeMillis());
        } catch (IOException e) {
        }
        byte[] b = new byte[dbos.getCount()];
        System.arraycopy(dbos.getBuffer(), 0, b, 0, b.length);
        return b;
    }

    public Serializable createResponse(Serializable challenge, String password) {
        byte[] cb = challenge.toString().getBytes();
        byte[] pb = password == null ? "null".getBytes() : password.getBytes();
        byte[] rb = new byte[cb.length + pb.length];
        System.arraycopy(cb, 0, rb, 0, cb.length);
        System.arraycopy(pb, 0, rb, cb.length, pb.length);
        // Don't sort, otherwise each password that
        // has the same letters as the correct password, regardless of order
        if (password == null) // Only for comnpatibility reasons...
            Arrays.sort(rb);
        byte[] resp = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(rb);
            resp = md.digest();
        } catch (Exception e) {
            resp = rb;
        }
        return resp;
    }

    public byte[] createBytesResponse(byte[] challenge, String password) {
        byte[] pb = password == null ? "null".getBytes() : password.getBytes();
        byte[] rb = new byte[challenge.length + pb.length];
        System.arraycopy(challenge, 0, rb, 0, challenge.length);
        System.arraycopy(pb, 0, rb, challenge.length, pb.length);
        byte[] resp = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(rb);
            resp = md.digest();
        } catch (Exception e) {
            resp = rb;
        }
        return resp;
    }

    public boolean verifyResponse(Serializable challenge, Serializable response, String password) {
        String rs = new String((byte[]) response);
        String cr = new String((byte[]) createResponse(challenge, password));
        return cr.equals(rs);
    }

    public boolean verifyBytesResponse(byte[] challenge, byte[] response, String password) {
        String rs = new String(response);
        String cr = new String(createBytesResponse(challenge, password));
        return cr.equals(rs);
    }
}

