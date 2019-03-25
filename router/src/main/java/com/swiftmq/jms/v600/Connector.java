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

package com.swiftmq.jms.v600;

import com.swiftmq.client.thread.PoolManager;
import com.swiftmq.jms.smqp.v600.SMQPBulkRequest;
import com.swiftmq.jms.smqp.v600.SMQPFactory;
import com.swiftmq.jms.v600.po.*;
import com.swiftmq.net.client.Connection;
import com.swiftmq.net.client.ExceptionHandler;
import com.swiftmq.net.client.InboundHandler;
import com.swiftmq.net.client.Reconnector;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRegistry;
import com.swiftmq.tools.timer.TimerEvent;
import com.swiftmq.tools.timer.TimerListener;
import com.swiftmq.tools.timer.TimerRegistry;
import com.swiftmq.tools.util.DataStreamOutputStream;
import com.swiftmq.tools.util.LengthCaptureDataInput;

import java.io.DataInput;
import java.io.IOException;
import java.util.Date;
import java.util.List;

public class Connector implements ReconnectVisitor, InboundHandler, ExceptionHandler {
    DumpableFactory dumpableFactory = new com.swiftmq.jms.smqp.SMQPFactory(new com.swiftmq.jms.smqp.v600.SMQPFactory());

    PipelineQueue pipelineQueue = null;
    Reconnector reconnector = null;
    RecreatableConnection recreatableConnection = null;
    boolean reconnectInProgress = false;
    boolean debug = false;
    Connection connection = null;
    DataStreamOutputStream outStream = null;
    boolean ok = false;
    Semaphore sem = null;
    Request current = null;
    long requestTime = -1;
    volatile boolean recreateStarted = false;
    PORecreate currentRecreatePO = null;
    boolean closed = false;

    public Connector(Reconnector reconnector) {
        pipelineQueue = new PipelineQueue(PoolManager.getInstance().getConnectionPool(), "Connector", this);
        this.reconnector = reconnector;
        this.debug = reconnector.isDebug();
        if (debug) System.out.println(toString() + ", created");
    }

    public void dispatch(POObject po) {
        pipelineQueue.enqueue(po);
    }

    public void dataAvailable(LengthCaptureDataInput in) {
        dispatch(new PODataAvailable(in));
    }

    public void onException(IOException exception) {
        dispatch(new POException(exception));
    }

    private void writeObject(Dumpable obj) throws IOException {
        if (debug) System.out.println(toString() + ", writeObject, obj=" + obj);
        Dumpalizer.dump(outStream, obj);
        outStream.flush();
        requestTime = System.currentTimeMillis();
        TimerRegistry.Singleton().addInstantTimerListener(RequestRegistry.SWIFTMQ_REQUEST_TIMEOUT, new Timeout());
    }

    private void setReply(Dumpable obj) throws Exception {
        switch (current.getDumpId()) {
            case com.swiftmq.jms.smqp.SMQPFactory.DID_SMQP_VERSION_REQ:
                recreatableConnection.setVersionReply((Reply) obj);
                dispatch(new POAuthenticateRequest());
                break;
            case SMQPFactory.DID_GETAUTHCHALLENGE_REQ:
                recreatableConnection.setAuthenticateReply((Reply) obj);
                dispatch(new POAuthenticateResponse());
                break;
            case SMQPFactory.DID_AUTHRESPONSE_REQ:
                recreatableConnection.setAuthenticateResponseReply((Reply) obj);
                dispatch(new POMetaDataRequest());
                break;
            case SMQPFactory.DID_GETMETADATA_REQ:
                recreatableConnection.setMetaDataReply((Reply) obj);
                dispatch(new POGetClientIdRequest());
                break;
            case SMQPFactory.DID_GETCLIENTID_REQ:
                recreatableConnection.setGetClientIdReply((Reply) obj);
                dispatch(new POSetClientIdRequest());
                break;
            case SMQPFactory.DID_SETCLIENTID_REQ:
                recreatableConnection.setSetClientIdReply((Reply) obj);
                new Recreator().start();
                break;
            default:
                break;
        }
    }

    public void visit(POReconnect po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (closed) {
            if (debug) System.out.println(toString() + ", visit, po=" + po + ", closed, return");
            if (po.getSemaphore() != null)
                po.getSemaphore().notifySingleWaiter();
            return;
        }
        if (reconnectInProgress && !po.isInternalRetry()) {
            if (debug) System.out.println(toString() + ", visit, po=" + po + ", reconnect already in progress");
            if (po.getSemaphore() != null)
                po.getSemaphore().notifySingleWaiter();
        } else {
            reconnectInProgress = true;
            recreateStarted = false;
            sem = po.getSemaphore();
            recreatableConnection = po.getRecreatableConnection();
            if (!po.isInternalRetry())
                recreatableConnection.prepareForReconnect();
            connection = reconnector.getConnection();
            if (connection != null) {
                if (debug) System.out.println(toString() + ", visit, po=" + po + ", connection=" + connection);
                connection.setInboundHandler(this);
                connection.setExceptionHandler(this);
                outStream = new DataStreamOutputStream(connection.getOutputStream());
                try {
                    connection.start();
                    dispatch(new POVersionRequest());
                } catch (Exception e) {
                    if (debug)
                        System.out.println(toString() + ", visit, po=" + po + ", exception connection.start()=" + e);
                    reconnector.invalidateConnection();
                    connection = null;
                    dispatch(new POHandover());
                }
            } else
                dispatch(new POHandover());
        }
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(PODataAvailable po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        DataInput in = po.getIn();
        try {
            Dumpable obj = Dumpalizer.construct(in, dumpableFactory);
            if (debug) System.out.println(toString() + ", dataAvailable, obj=" + obj);
            if (obj == null || obj.getDumpId() == SMQPFactory.DID_KEEPALIVE_REQ) {
                return;
            }
            if (!recreateStarted) {
                if (obj.getDumpId() == SMQPFactory.DID_BULK_REQ) {
                    SMQPBulkRequest bulkRequest = (SMQPBulkRequest) obj;
                    for (int i = 0; i < bulkRequest.len; i++) {
                        Dumpable dumpable = (Dumpable) bulkRequest.dumpables[i];
                        if (dumpable.getDumpId() != SMQPFactory.DID_KEEPALIVE_REQ)
                            setReply(dumpable);
                    }
                } else
                    setReply(obj);
                current = null;
            } else if (obj.getDumpId() == SMQPFactory.DID_BULK_REQ) {
                SMQPBulkRequest bulkRequest = (SMQPBulkRequest) obj;
                for (int i = 0; i < bulkRequest.len; i++) {
                    Dumpable dumpable = (Dumpable) bulkRequest.dumpables[i];
                    if (dumpable.getDumpId() != SMQPFactory.DID_KEEPALIVE_REQ) {
                        currentRecreatePO.getRecreatable().setRecreateReply((Reply) dumpable);
                        currentRecreatePO.setSuccess(true);
                        if (currentRecreatePO.getSemaphore() != null)
                            currentRecreatePO.getSemaphore().notifySingleWaiter();
                    }
                }
            } else {
                currentRecreatePO.getRecreatable().setRecreateReply((Reply) obj);
                currentRecreatePO.setSuccess(true);
                if (currentRecreatePO.getSemaphore() != null)
                    currentRecreatePO.getSemaphore().notifySingleWaiter();
            }
            currentRecreatePO = null;
            requestTime = -1;
        } catch (Exception e) {
            if (debug) System.out.println(toString() + ", visit, po=" + po + ", exception=" + e);
            if (currentRecreatePO != null) {
                currentRecreatePO.setSuccess(false);
                currentRecreatePO.setException(e.toString());
                if (currentRecreatePO.getSemaphore() != null)
                    currentRecreatePO.getSemaphore().notifySingleWaiter();
                currentRecreatePO = null;
            }
            reconnector.invalidateConnection();
            connection = null;
            reconnectInProgress = false;
            currentRecreatePO = null;
            requestTime = -1;
            dispatch(new POReconnect(sem, recreatableConnection, true));
        }
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(POException po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (currentRecreatePO != null) {
            currentRecreatePO.setSuccess(false);
            currentRecreatePO.setException(po.getException());
            if (currentRecreatePO.getSemaphore() != null)
                currentRecreatePO.getSemaphore().notifySingleWaiter();
            currentRecreatePO = null;
        }
        reconnector.invalidateConnection();
        connection = null;
        reconnectInProgress = false;
        requestTime = -1;
        dispatch(new POReconnect(sem, recreatableConnection, true));
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(POTimeoutCheck po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (closed)
            return;
        if (requestTime != -1 && System.currentTimeMillis() - requestTime > RequestRegistry.SWIFTMQ_REQUEST_TIMEOUT) {
            if (debug) System.out.println(toString() + ", visit, po=" + po + ", timeout detected!");
            if (currentRecreatePO != null) {
                currentRecreatePO.setSuccess(false);
                currentRecreatePO.setException("Timeout detected");
                currentRecreatePO.getSemaphore().notifySingleWaiter();
                currentRecreatePO = null;
            }
            reconnector.invalidateConnection();
            connection = null;
            reconnectInProgress = false;
            requestTime = -1;
            dispatch(new POReconnect(sem, recreatableConnection, true));
        }
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(POVersionRequest po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (closed)
            return;
        current = recreatableConnection.getVersionRequest();
        try {
            writeObject(current);
        } catch (IOException e) {
            if (debug) System.out.println(toString() + ", visit, po=" + po + ", writeObject=" + e);
            reconnector.invalidateConnection();
            connection = null;
            reconnectInProgress = false;
            dispatch(new POReconnect(sem, recreatableConnection, true));
        }
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(POAuthenticateRequest po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (closed)
            return;
        current = recreatableConnection.getAuthenticateRequest();
        try {
            writeObject(current);
        } catch (IOException e) {
            reconnector.invalidateConnection();
            connection = null;
            reconnectInProgress = false;
            dispatch(new POReconnect(sem, recreatableConnection, true));
        }
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(POAuthenticateResponse po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (closed)
            return;
        current = recreatableConnection.getAuthenticateResponse();
        try {
            writeObject(current);
        } catch (IOException e) {
            if (debug) System.out.println(toString() + ", visit, po=" + po + ", writeObject=" + e);
            reconnector.invalidateConnection();
            connection = null;
            reconnectInProgress = false;
            dispatch(new POReconnect(sem, recreatableConnection, true));
        }
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(POMetaDataRequest po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (closed)
            return;
        current = recreatableConnection.getMetaDataRequest();
        try {
            writeObject(current);
        } catch (IOException e) {
            if (debug) System.out.println(toString() + ", visit, po=" + po + ", writeObject=" + e);
            reconnector.invalidateConnection();
            connection = null;
            reconnectInProgress = false;
            dispatch(new POReconnect(sem, recreatableConnection, true));
        }
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(POGetClientIdRequest po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (closed)
            return;
        current = recreatableConnection.getGetClientIdRequest();
        if (current != null) {
            try {
                writeObject(current);
            } catch (IOException e) {
                if (debug) System.out.println(toString() + ", visit, po=" + po + ", writeObject=" + e);
                reconnector.invalidateConnection();
                connection = null;
                reconnectInProgress = false;
                dispatch(new POReconnect(sem, recreatableConnection, true));
            }
        } else
            new Recreator().start();
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(POSetClientIdRequest po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (closed)
            return;
        current = recreatableConnection.getSetClientIdRequest();
        if (current != null) {
            try {
                writeObject(current);
            } catch (IOException e) {
                if (debug) System.out.println(toString() + ", visit, po=" + po + ", writeObject=" + e);
                reconnector.invalidateConnection();
                connection = null;
                reconnectInProgress = false;
                dispatch(new POReconnect(sem, recreatableConnection, true));
            }
        } else
            new Recreator().start();
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(PORecreate po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (closed)
            return;
        currentRecreatePO = po;
        try {
            writeObject(po.getRequest());
        } catch (IOException e) {
            if (debug) System.out.println(toString() + ", visit, po=" + po + ", writeObject=" + e);
            po.setSuccess(false);
            po.getSemaphore().notifySingleWaiter();
            reconnector.invalidateConnection();
            connection = null;
            reconnectInProgress = false;
            dispatch(new POReconnect(sem, recreatableConnection, true));
        }
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(POHandover po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        if (!closed) {
            if (connection != null || sem != null) {
                recreatableConnection.handOver(connection);
            } else {
                recreatableConnection.cancelAndNotify(new Exception("Unable to connect to " + reconnector.getServers() + ", max. retries reached!"), false);
                dispatch(new POClose(null));
            }
        }
        reconnectInProgress = false;
        if (sem != null)
            sem.notifySingleWaiter();
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void visit(POClose po) {
        if (debug) System.out.println(toString() + ", visit, po=" + po + " ...");
        closed = true;
        if (reconnectInProgress) {
            if (sem != null)
                sem.notifySingleWaiter();
        }
        pipelineQueue.close();
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();
        if (debug) System.out.println(toString() + ", visit, po=" + po + " done");
    }

    public void close() {
        if (debug) System.out.println(toString() + ", close ...");
        Semaphore sem = new Semaphore();
        dispatch(new POClose(sem));
        sem.waitHere();
        if (debug) System.out.println(toString() + ", close done");
    }

    public String toString() {
        return new Date() + " [Connector, reconnector=" + reconnector + "]";
    }

    private class Recreator extends Thread {
        private boolean recreate(Recreatable recreatable) {
            if (closed)
                return false;
            Request request = recreatable.getRecreateRequest();
            if (request != null) {
                Semaphore sem = new Semaphore();
                PORecreate po = new PORecreate(sem, recreatable, request);
                dispatch(po);
                sem.waitHere();
                if (!po.isSuccess())
                    return false;
            }
            List list = recreatable.getRecreatables();
            if (list != null && list.size() > 0) {
                for (int i = 0; i < list.size(); i++)
                    if (!recreate((Recreatable) list.get(i)))
                        return false;
            }
            return true;
        }

        public void run() {
            if (debug) System.out.println(Connector.this.toString() + "/Recreator, started");
            recreateStarted = true;
            boolean b = recreate(recreatableConnection);
            recreateStarted = false;
            if (b)
                dispatch(new POHandover());
            if (debug) System.out.println(Connector.this.toString() + "/Recreator, stopped");
        }
    }

    private class Timeout implements TimerListener {
        public void performTimeAction(TimerEvent evt) {
            if (debug) System.out.println(new Date() + "/" + evt);
            dispatch(new POTimeoutCheck());
        }
    }
}
