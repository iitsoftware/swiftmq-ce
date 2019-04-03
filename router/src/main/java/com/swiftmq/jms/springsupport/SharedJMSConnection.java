package com.swiftmq.jms.springsupport;

import javax.jms.*;
import javax.jms.Queue;
import java.util.*;

public class SharedJMSConnection
        implements Connection, QueueConnection, TopicConnection {
    static final boolean DEBUG = Boolean.valueOf(System.getProperty("swiftmq.springsupport.debug", "false")).booleanValue();
    Connection internalConnection = null;
    long poolExpiration = 0;
    boolean firstTransacted = false;
    int firstAckMode = -1;
    List pool = new LinkedList();
    Timer timer = new Timer(true);
    TimerTask expiryChecker = null;

    public SharedJMSConnection(Connection internalConnection, long poolExpiration) {
        this.internalConnection = internalConnection;
        this.poolExpiration = poolExpiration;
        if (poolExpiration > 0) {
            long delay = poolExpiration + 500;
            expiryChecker = new TimerTask() {
                public void run() {
                    checkExpired();
                }
            };
            timer.schedule(expiryChecker, delay, delay);
        }
        if (DEBUG) System.out.println(toString() + "/created");
    }

    public long getPoolExpiration() {
        return poolExpiration;
    }

    public synchronized Session createSession(boolean transacted, int ackMode) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createSession, poolSize=" + pool.size());
        if (pool.size() > 0) {
            PoolEntry entry = (PoolEntry) pool.remove(0);
            if (DEBUG) System.out.println(toString() + "/createSession, returning session from pool: " + entry);
            return entry.pooledSession;
        }
        if (firstAckMode == -1) {
            firstTransacted = transacted;
            firstAckMode = ackMode;
        } else {
            if (transacted != firstTransacted || ackMode != firstAckMode)
                throw new javax.jms.IllegalStateException("SharedJMSConnection: all JMS session must have the same transacted flag and ackMode!");
        }
        if (DEBUG) System.out.println(toString() + "/createSession, returning a new session");
        return new PooledSession(this, internalConnection.createSession(transacted, ackMode));
    }

    protected synchronized void checkIn(PooledSession pooledSession) {
        PoolEntry entry = new PoolEntry(System.currentTimeMillis(), pooledSession);
        if (pool.size() == 0)
            pool.add(entry);
        else
            pool.add(0, entry);
        if (DEBUG) System.out.println(toString() + "/checkIn, poolSize=" + pool.size() + ", entry=" + entry);
    }

    public synchronized void checkExpired() {
        if (DEBUG) System.out.println(toString() + "/checkExpired, poolSize=" + pool.size());
        if (pool.size() == 0)
            return;
        for (Iterator iter = pool.iterator(); iter.hasNext(); ) {
            PoolEntry entry = (PoolEntry) iter.next();
            long now = System.currentTimeMillis();
            if (DEBUG)
                System.out.println(toString() + "/checkExpired, now=" + now + ", expTime=" + (entry.poolInsertionTime + poolExpiration));
            entry.pooledSession.checkExpired();
            if (entry.poolInsertionTime + poolExpiration <= now) {
                try {
                    if (DEBUG) System.out.println(toString() + "/checkExpired, closing session=" + entry.pooledSession);
                    entry.pooledSession.closeInternal();
                } catch (JMSException e) {
                }
                iter.remove();
            }
        }
    }

    public QueueSession createQueueSession(boolean transacted, int ackMode) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createQueueSession");
        return (QueueSession) createSession(transacted, ackMode);
    }

    public TopicSession createTopicSession(boolean transacted, int ackMode) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createTopicSession");
        return (TopicSession) createSession(transacted, ackMode);
    }

    public String getClientID() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/getClientID");
        return internalConnection.getClientID();
    }

    public void setClientID(String cid) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/setClientID, id=" + cid);
        internalConnection.setClientID(cid);
    }

    public ConnectionMetaData getMetaData() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/getMetaData");
        return internalConnection.getMetaData();
    }

    public ExceptionListener getExceptionListener() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/getExceptionListener");
        return internalConnection.getExceptionListener();
    }

    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/setExceptionListener");
        internalConnection.setExceptionListener(exceptionListener);
    }

    public void start() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/start");
        internalConnection.start();
    }

    public void stop() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/stop");
        internalConnection.stop();
    }

    public void close() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/close (ignore)");
    }

    public synchronized void destroy() throws Exception {
        if (DEBUG) System.out.println(toString() + "/destroy");
        if (pool.size() > 0) {
            for (Iterator iter = pool.iterator(); iter.hasNext(); ) {
                PoolEntry entry = (PoolEntry) iter.next();
                if (DEBUG) System.out.println(toString() + "/destroy, closing session=" + entry.pooledSession);
                entry.pooledSession.closeInternal();
                iter.remove();
            }
        }
        internalConnection.close();
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String string, ServerSessionPool serverSessionPool, int i) throws JMSException {
        throw new javax.jms.IllegalStateException("SharedJMSConnection: operation is not supported!");
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String string, String string1, ServerSessionPool serverSessionPool, int i) throws JMSException {
        throw new javax.jms.IllegalStateException("SharedJMSConnection: operation is not supported!");
    }

    public ConnectionConsumer createConnectionConsumer(Queue queue, String string, ServerSessionPool serverSessionPool, int i) throws JMSException {
        throw new javax.jms.IllegalStateException("SharedJMSConnection: operation is not supported!");
    }

    public ConnectionConsumer createConnectionConsumer(Topic topic, String string, ServerSessionPool serverSessionPool, int i) throws JMSException {
        throw new javax.jms.IllegalStateException("SharedJMSConnection: operation is not supported!");
    }

    public String toString() {
        return "/SharedJMSConnection";
    }

    private class PoolEntry {
        long poolInsertionTime = 0;
        PooledSession pooledSession = null;

        public PoolEntry(long poolInsertionTime, PooledSession pooledSession) {
            this.poolInsertionTime = poolInsertionTime;
            this.pooledSession = pooledSession;
        }

        public String toString() {
            return "/PoolEntry, insertionTime=" + poolInsertionTime + ", pooledSession=" + pooledSession;
        }
    }
}
