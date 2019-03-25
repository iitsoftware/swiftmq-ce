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

package com.swiftmq.jms.smqp.v500;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.*;

public class GetAuthChallengeReply extends Reply {
    Serializable challenge = null;
    String challengeResponseFactoryClass = null;


    /**
     * Returns a unique dump id for this object.
     *
     * @return unique dump id
     */
    public int getDumpId() {
        return SMQPFactory.DID_GET_AUTH_CHALLENGE_REP;
    }

    /**
     * Write the content of this object to the stream.
     *
     * @param out output stream
     * @throws IOException if an error occurs
     */
    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);

        if (challenge == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            DataByteArrayOutputStream dos = new DataByteArrayOutputStream(256);
            (new ObjectOutputStream(dos)).writeObject(challenge);
            out.writeInt(dos.getCount());
            out.write(dos.getBuffer(), 0, dos.getCount());
        }

        if (challengeResponseFactoryClass == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            out.writeUTF(challengeResponseFactoryClass);
        }
    }

    /**
     * Read the content of this object from the stream.
     *
     * @param in input stream
     * @throws IOException if an error occurs
     */
    public void readContent(DataInput in) throws IOException {
        super.readContent(in);

        byte set = in.readByte();

        if (set == 0) {
            challenge = null;
        } else {
            try {
                byte[] b = new byte[in.readInt()];
                in.readFully(b);
                challenge = (Serializable) (new ObjectInputStream(new DataByteArrayInputStream(b))).readObject();
            } catch (ClassNotFoundException ignored) {
            }
        }

        set = in.readByte();

        if (set == 0) {
            challengeResponseFactoryClass = null;
        } else {
            challengeResponseFactoryClass = in.readUTF();
        }
    }

    /**
     * @return
     * @SBGen Method get challenge
     */
    public Serializable getChallenge() {
        // SBgen: Get variable
        return (challenge);
    }

    public void setChallenge(Serializable challenge) {
        this.challenge = challenge;
    }

    /**
     * @return
     * @SBGen Method get challengeResponseFactoryClass
     */
    public String getChallengeResponseFactoryClass() {
        // SBgen: Get variable
        return (challengeResponseFactoryClass);
    }

    public void setChallengeResponseFactoryClass(String challengeResponseFactoryClass) {
        this.challengeResponseFactoryClass = challengeResponseFactoryClass;
    }

    public String toString() {
        return "[GetAuthChallengeReply " + super.toString() + " challenge="
                + challenge + " challengeResponseFactoryClass=" + challengeResponseFactoryClass + "]";
    }
}

