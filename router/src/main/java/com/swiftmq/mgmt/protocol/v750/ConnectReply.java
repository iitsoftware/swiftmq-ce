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

package com.swiftmq.mgmt.protocol.v750;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.*;

public class ConnectReply extends Reply {
    String routerName = null;
    boolean authRequired = false;
    String crFactory = null;
    Serializable challenge = null;
    long leaseTimeout = 0;

    public ConnectReply(String routerName, boolean authRequired, String crFactory, Serializable challenge, long leaseTimeout) {
        this.routerName = routerName;
        this.authRequired = authRequired;
        this.crFactory = crFactory;
        this.challenge = challenge;
        this.leaseTimeout = leaseTimeout;
    }

    public ConnectReply() {
    }

    public String getRouterName() {
        return routerName;
    }

    public void setRouterName(String routerName) {
        this.routerName = routerName;
    }

    public boolean isAuthRequired() {
        return authRequired;
    }

    public void setAuthRequired(boolean authRequired) {
        this.authRequired = authRequired;
    }

    public String getCrFactory() {
        return crFactory;
    }

    public void setCrFactory(String crFactory) {
        this.crFactory = crFactory;
    }

    public Serializable getChallenge() {
        return challenge;
    }

    public void setChallenge(Serializable challenge) {
        this.challenge = challenge;
    }

    public long getLeaseTimeout() {
        return leaseTimeout;
    }

    public void setLeaseTimeout(long leaseTimeout) {
        this.leaseTimeout = leaseTimeout;
    }

    public int getDumpId() {
        return ProtocolFactory.CONNECT_REP;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        out.writeUTF(routerName);
        out.writeBoolean(authRequired);
        if (authRequired) {
            out.writeUTF(crFactory);
            DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(dbos);
            oos.writeObject(challenge);
            oos.flush();
            oos.close();
            out.writeInt(dbos.getCount());
            out.write(dbos.getBuffer(), 0, dbos.getCount());
        }
        out.writeLong(leaseTimeout);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        routerName = in.readUTF();
        authRequired = in.readBoolean();
        if (authRequired) {
            crFactory = in.readUTF();
            byte b[] = new byte[in.readInt()];
            in.readFully(b);
            DataByteArrayInputStream dbis = new DataByteArrayInputStream(b);
            ObjectInputStream ois = new ObjectInputStream(dbis);
            try {
                challenge = (Serializable) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e.toString());
            }
            ois.close();
        }
        leaseTimeout = in.readLong();
    }

    public String toString() {
        return "[ConnectReply " + super.toString() + ", routerName=" + routerName + ", authRequired=" + authRequired +
                ", crFactory=" + crFactory + ", challenge=" + challenge + ", leaseTimeout=" + leaseTimeout + "]";
    }
}
