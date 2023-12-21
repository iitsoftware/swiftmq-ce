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

package com.swiftmq.impl.amqp.sasl.v01_00_00;

import com.swiftmq.amqp.v100.generated.FrameReader;
import com.swiftmq.amqp.v100.generated.security.sasl.*;
import com.swiftmq.amqp.v100.transport.AMQPFrame;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.impl.amqp.Handler;
import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.VersionedConnection;
import com.swiftmq.impl.amqp.sasl.provider.AnonServer;
import com.swiftmq.impl.amqp.sasl.v01_00_00.po.POClose;
import com.swiftmq.impl.amqp.sasl.v01_00_00.po.POSaslFrameReceived;
import com.swiftmq.impl.amqp.sasl.v01_00_00.po.POSendInit;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.util.LengthCaptureDataInput;

import javax.security.auth.callback.*;
import javax.security.sasl.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SASLHandler extends SaslFrameVisitorAdapter implements Handler, SASLVisitor {
    SwiftletContext ctx = null;
    VersionedConnection versionedConnection = null;
    final AtomicBoolean closed = new AtomicBoolean(false);
    final AtomicBoolean closeInProgress = new AtomicBoolean(false);
    Property authEnabled = null;
    AMQPType[] saslMechanisms = null;
    String hostname = null;
    SaslServer saslServer = null;
    String userName = "anonymous";
    String realm = null;
    ActiveLogin activeLogin = null;
    EventLoop eventLoop;

    public SASLHandler(SwiftletContext ctx, VersionedConnection versionedConnection) {
        this.ctx = ctx;
        this.versionedConnection = versionedConnection;
        authEnabled = SwiftletManager.getInstance().getConfiguration("sys$authentication").getProperty("authentication-enabled");
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
        }
        eventLoop = ctx.threadpoolSwiftlet.createEventLoop("sys$amqp.sasl.service", list -> list.forEach(e -> ((POObject) e).accept(SASLHandler.this)));
        new POSendInit().accept(this);  // Must be done this way, otherwise SaslOutcome message may outrun the SASL prot header
    }

    private AMQPType[] createSaslMechanisms() {
        List l = new ArrayList();
        for (Enumeration _enum = Sasl.getSaslServerFactories(); _enum.hasMoreElements(); ) {
            SaslServerFactory sf = (SaslServerFactory) _enum.nextElement();
            String[] mnames = sf.getMechanismNames(null);
            if (mnames != null) {
                for (int i = 0; i < mnames.length; i++) {
                    if (!mnames[i].toUpperCase().equals("GSSAPI")) {
                        if (mnames[i].endsWith(AnonServer.MECHNAME)) {
                            if (!((Boolean) authEnabled.getValue()).booleanValue())
                                l.add(new AMQPSymbol(mnames[i]));
                        } else
                            l.add(new AMQPSymbol(mnames[i]));
                    }
                }
            }
        }
        return (AMQPType[]) l.toArray(new AMQPType[l.size()]);
    }

    private boolean hasMechanism(String mechanism) {
        for (int i = 0; i < saslMechanisms.length; i++)
            if (((AMQPSymbol) saslMechanisms[i]).getValue().equals(mechanism))
                return true;
        return false;
    }

    private int nextOccurance(byte[] src, int pos, byte b) {
        for (int i = pos; i < src.length; i++)
            if (src[i] == b)
                return i;
        return src.length;
    }

    private String[] parseResponse(byte[] response) {
        String[] s = new String[3];
        try {
            int idx = 0;
            int x0 = nextOccurance(response, 0, (byte) 0);
            if (x0 > 0)
                s[idx] = new String(response, 1, x0, "UTF-8");
            int x1 = nextOccurance(response, x0 + 1, (byte) 0);
            s[++idx] = new String(response, x0 + 1, x1 - (x0 + 1), "UTF-8");
            int x2 = nextOccurance(response, x1 + 1, (byte) 0);
            s[++idx] = new String(response, x1 + 1, x2 - (x1 + 1), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return s;
    }

    public void dataAvailable(LengthCaptureDataInput in) {
        try {
            SaslFrameIF frame = FrameReader.createSaslFrame(in);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", dataAvailable, frame=" + frame);
            if (ctx.protSpace.enabled)
                ctx.protSpace.trace("amqp-100", toString() + "/RCV[" + ((AMQPFrame) frame).getChannel() + "] (size=" + frame.getPredictedSize() + "): " + frame);
            new POSaslFrameReceived(frame).accept(this);   // Important to do that in the same thread, otherwise the AMQPInputHandler won't work
        } catch (Exception e) {
            e.printStackTrace();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", dataAvailable, exception=" + e);
            ctx.logSwiftlet.logError(ctx.amqpSwiftlet.getName(), toString() + ", dataAvailable, exception=" + e);
            ctx.networkSwiftlet.getConnectionManager().removeConnection(versionedConnection.getConnection());
        }
    }

    public void dispatch(POObject po) {
        eventLoop.submit(po);
    }

    public String getVersion() {
        return null;
    }

    public void collect(long lastCollect) {
        // do nothing
    }

    public void visit(POSendInit po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        try {
            saslMechanisms = createSaslMechanisms();
            SaslMechanismsFrame frame = new SaslMechanismsFrame(0);
            frame.setSaslServerMechanisms(new AMQPArray(AMQPTypeDecoder.SYM8, saslMechanisms));
            versionedConnection.send(SASLHandlerFactory.SASL_INIT);
            versionedConnection.send(frame);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POSaslFrameReceived po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        po.getFrame().accept(this);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POClose po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        closed.set(true);
        po.setSuccess(true);
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    private void processResponse(byte[] response) {
        try {
            byte[] challenge = saslServer.evaluateResponse(response);
            if (saslServer.isComplete()) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", processResponse, complete, userName: " + userName + ", realm: " + realm);
                activeLogin = ctx.authSwiftlet.createActiveLogin(userName, "AMQP");
                SaslOutcomeFrame outcomeFrame = new SaslOutcomeFrame(0);
                outcomeFrame.setMycode(SaslCode.OK);
                versionedConnection.send(outcomeFrame);
                versionedConnection.setSaslFinished(true, activeLogin);
                dispatch(new POClose(null));
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", processResponse, not complete");
                SaslChallengeFrame saslChallengeFrame = new SaslChallengeFrame(0);
                saslChallengeFrame.setChallenge(new AMQPBinary(challenge));
                versionedConnection.send(saslChallengeFrame);
            }
        } catch (SaslException e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", processResponse, exception: " + e);
            SaslOutcomeFrame outcomeFrame = new SaslOutcomeFrame(0);
            outcomeFrame.setMycode(SaslCode.AUTH);
            versionedConnection.send(outcomeFrame);
            ctx.networkSwiftlet.getConnectionManager().removeConnection(versionedConnection.getConnection());
        }
    }

    public void visit(SaslInitFrame frame) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit SaslInitFrame ...");
        SaslCode code = null;
        String mechanism = frame.getMechanism().getValue();
        try {
            saslServer = Sasl.createSaslServer(mechanism, "AMQP", frame.getHostname() == null ? SwiftletManager.getInstance().getRouterName() : frame.getHostname().getValue(), null, new CallbackHandlerImpl());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit SaslInitFrame, saslServer: " + saslServer);
            processResponse(frame.getInitialResponse() != null ? frame.getInitialResponse().getValue() : new byte[0]);
        } catch (SaslException e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", SaslInitFrame, exception: " + e);
            SaslOutcomeFrame outcomeFrame = new SaslOutcomeFrame(0);
            outcomeFrame.setMycode(SaslCode.SYS);
            versionedConnection.send(outcomeFrame);
            ctx.networkSwiftlet.getConnectionManager().removeConnection(versionedConnection.getConnection());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit SaslInitFrame done");
    }

    public void visit(SaslResponseFrame frame) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit SaslResponseFrame ...");
        processResponse(frame.getResponse().getValue());
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit SaslResponseFrame done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close ...");
        if (closed.get() || closeInProgress.getAndSet(true)) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", closed or close in progress, return");
            return;
        }
        Semaphore sem = new Semaphore();
        dispatch(new POClose(sem));
        sem.waitHere();
        eventLoop.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close done");
    }

    public String toString() {
        return versionedConnection.toString() + "/SASLHandler";
    }

    private class CallbackHandlerImpl implements CallbackHandler {
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", CallbackHandlerImpl.handle ...");
            PasswordCallback pwc = null;
            AuthorizeCallback azc = null;
            for (Callback c : callbacks) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", CallbackHandlerImpl.handle, c=" + c);
                if (c instanceof NameCallback)
                    userName = ((NameCallback) c).getDefaultName();
                else if (c instanceof PasswordCallback)
                    pwc = (PasswordCallback) c;
                else if (c instanceof RealmCallback)
                    realm = ((RealmCallback) c).getDefaultText();
                else if (c instanceof AuthorizeCallback)
                    azc = (AuthorizeCallback) c;
                else
                    throw new UnsupportedEncodingException(c.getClass().getName());
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", CallbackHandlerImpl.handle, userName=" + userName);
            if (userName != null) {
                try {
                    String password = ctx.authSwiftlet.getPassword(userName);
                    if (pwc != null)
                        pwc.setPassword(password.toCharArray());
                    if (azc != null) {
                        azc.setAuthorized(true);
                        azc.setAuthorizedID(userName);
                    }
                } catch (AuthenticationException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", CallbackHandlerImpl.handle, exception=" + e);
                    if (azc != null)
                        azc.setAuthorized(false);
                }
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", CallbackHandlerImpl.handle done");
        }
    }
}
