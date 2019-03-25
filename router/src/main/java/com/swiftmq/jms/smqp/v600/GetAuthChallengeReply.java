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

package com.swiftmq.jms.smqp.v600;

/**
 * SMQP-Protocol Version 600, Class: GetAuthChallengeReply
 * Automatically generated, don't change!
 * Generation Date: Thu Feb 09 09:59:46 CET 2006
 * (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.ReplyNE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GetAuthChallengeReply extends ReplyNE {
    private byte[] challenge;
    private String factoryClass;

    public GetAuthChallengeReply(byte[] challenge, String factoryClass) {
        this.challenge = challenge;
        this.factoryClass = factoryClass;
    }

    protected GetAuthChallengeReply() {
    }

    public void setChallenge(byte[] challenge) {
        this.challenge = challenge;
    }

    public byte[] getChallenge() {
        return challenge;
    }

    public void setFactoryClass(String factoryClass) {
        this.factoryClass = factoryClass;
    }

    public String getFactoryClass() {
        return factoryClass;
    }

    public int getDumpId() {
        return SMQPFactory.DID_GETAUTHCHALLENGE_REP;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        if (challenge != null) {
            out.writeBoolean(true);
            SMQPUtil.write(challenge, out);
        } else
            out.writeBoolean(false);
        if (factoryClass != null) {
            out.writeBoolean(true);
            SMQPUtil.write(factoryClass, out);
        } else
            out.writeBoolean(false);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        boolean challenge_set = in.readBoolean();
        if (challenge_set)
            challenge = SMQPUtil.read(challenge, in);
        boolean factoryClass_set = in.readBoolean();
        if (factoryClass_set)
            factoryClass = SMQPUtil.read(factoryClass, in);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v600/GetAuthChallengeReply, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("challenge=");
        _b.append(challenge);
        _b.append(", ");
        _b.append("factoryClass=");
        _b.append(factoryClass);
        _b.append("]");
        return _b.toString();
    }
}
