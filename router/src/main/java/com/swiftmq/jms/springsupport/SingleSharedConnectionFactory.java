package com.swiftmq.jms.springsupport;

import javax.jms.*;

public class SingleSharedConnectionFactory
        implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory {
    static final boolean DEBUG = Boolean.valueOf(System.getProperty("swiftmq.springsupport.debug", "false")).booleanValue();
    private ConnectionFactory targetConnectionFactory = null;
    private SharedJMSConnection sharedConnection = null;
    private long poolExpiration = 60000;
    private String clientId = null;

    public SingleSharedConnectionFactory() {
        if (DEBUG) System.out.println(toString() + "/created");
    }

    public SingleSharedConnectionFactory(ConnectionFactory targetConnectionFactory) {
        if (DEBUG) System.out.println(toString() + "/created");
        this.targetConnectionFactory = targetConnectionFactory;
    }

    public ConnectionFactory getTargetConnectionFactory() {
        return targetConnectionFactory;
    }

    public void setTargetConnectionFactory(ConnectionFactory targetConnectionFactory) {
        this.targetConnectionFactory = targetConnectionFactory;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public long getPoolExpiration() {
        return poolExpiration;
    }

    public void setPoolExpiration(long poolExpiration) {
        this.poolExpiration = poolExpiration;
    }

    private void ensureConnection() throws JMSException {
        if (sharedConnection == null) {
            if (DEBUG) System.out.println(toString() + "/ensureConnection, create connection ...");
            if (targetConnectionFactory == null)
                throw new javax.jms.IllegalStateException("SingleSharedConnectionFactory: targetConnectionFactory has not been set!");
            sharedConnection = new SharedJMSConnection(targetConnectionFactory.createConnection(), poolExpiration);
            if (clientId != null)
                sharedConnection.setClientID(clientId);
            if (DEBUG) System.out.println(toString() + "/ensureConnection, create connection done");
        }
    }

    public synchronized Connection createConnection() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createConnection");
        ensureConnection();
        return sharedConnection;
    }

    public Connection createConnection(String user, String password) throws JMSException {
        throw new javax.jms.IllegalStateException("SingleSharedConnectionFactory: operation not supported!");
    }

    public QueueConnection createQueueConnection() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createQueueConnection");
        ensureConnection();
        return sharedConnection;
    }

    public QueueConnection createQueueConnection(String user, String password) throws JMSException {
        throw new javax.jms.IllegalStateException("SingleSharedConnectionFactory: operation not supported!");
    }

    public TopicConnection createTopicConnection() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createTopicConnection");
        ensureConnection();
        return sharedConnection;
    }

    public TopicConnection createTopicConnection(String user, String password) throws JMSException {
        throw new javax.jms.IllegalStateException("SingleSharedConnectionFactory: operation not supported!");
    }

    public synchronized void destroy() throws Exception {
        if (DEBUG) System.out.println(toString() + "/destroy");
        if (sharedConnection != null) {
            if (DEBUG) System.out.println(toString() + "/destroy, close shared connection ...");
            sharedConnection.destroy();
            sharedConnection = null;
            if (DEBUG) System.out.println(toString() + "/destroy, close shared connection done");
        }
    }

    public String toString() {
        return "/SingleSharedConnectionFactory";
    }
}
