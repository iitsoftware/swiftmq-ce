package com.swiftmq.jms.springsupport;

import javax.jms.*;

public class PooledProducer
        implements QueueSender, TopicPublisher {
    static final boolean DEBUG = Boolean.valueOf(System.getProperty("swiftmq.springsupport.debug", "false")).booleanValue();
    PooledSession pooledSession = null;
    MessageProducer internalProducer = null;
    Destination internalDestination = null;
    long checkInTime = -1;

    public PooledProducer(PooledSession pooledSession, MessageProducer internalProducer, Destination internalDestination) {
        this.pooledSession = pooledSession;
        this.internalProducer = internalProducer;
        this.internalDestination = internalDestination;
        if (DEBUG) System.out.println(toString() + "/created");
    }

    public boolean getDisableMessageID() throws JMSException {
        return internalProducer.getDisableMessageID();
    }

    public void setDisableMessageID(boolean b) throws JMSException {
        internalProducer.setDisableMessageID(b);
    }

    public boolean getDisableMessageTimestamp() throws JMSException {
        return internalProducer.getDisableMessageTimestamp();
    }

    public void setDisableMessageTimestamp(boolean b) throws JMSException {
        internalProducer.setDisableMessageTimestamp(b);
    }

    public int getDeliveryMode() throws JMSException {
        return internalProducer.getDeliveryMode();
    }

    public void setDeliveryMode(int i) throws JMSException {
        internalProducer.setDeliveryMode(i);
    }

    public int getPriority() throws JMSException {
        return internalProducer.getPriority();
    }

    public void setPriority(int i) throws JMSException {
        internalProducer.setPriority(i);
    }

    public long getTimeToLive() throws JMSException {
        return internalProducer.getTimeToLive();
    }

    public void setTimeToLive(long l) throws JMSException {
        internalProducer.setTimeToLive(l);
    }

    public Destination getDestination() throws JMSException {
        return internalDestination;
    }

    public long getCheckInTime() {
        return checkInTime;
    }

    protected void closeInternal() {
        if (DEBUG) System.out.println(toString() + "/closeInternal");
        try {
            internalProducer.close();
        } catch (JMSException e) {
        }
    }

    public void close() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/close");
        checkInTime = System.currentTimeMillis();
        pooledSession.checkIn(this);
    }

    public void send(Destination destination, Message message) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/send, destination=" + destination + ", message=" + message);
        internalProducer.send(destination, message);
    }

    public void send(Destination destination, Message message, int i, int i1, long l) throws JMSException {
        if (DEBUG)
            System.out.println(toString() + "/send, destination=" + destination + ", message=" + message + ", i=" + i + ", i1=" + i1 + ", l=" + l);
        internalProducer.send(destination, message, i, i1, l);
    }

    public Queue getQueue() throws JMSException {
        return (Queue) internalDestination;
    }

    public void send(Message message) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/send, message=" + message);
        internalProducer.send(message);

    }

    public void send(Message message, int i, int i1, long l) throws JMSException {
        if (DEBUG)
            System.out.println(toString() + "/send, message=" + message + ", i=" + i + ", i1=" + i1 + ", l=" + l);
        internalProducer.send(message, i, i1, l);
    }

    public void send(Queue queue, Message message) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/send, queue=" + queue + ", message=" + message);
        internalProducer.send(queue, message);
    }

    public void send(Queue queue, Message message, int i, int i1, long l) throws JMSException {
        if (DEBUG)
            System.out.println(toString() + "/send, queue=" + queue + ", message=" + message + ", i=" + i + ", i1=" + i1 + ", l=" + l);
        internalProducer.send(queue, message, i, i1, l);
    }

    public Topic getTopic() throws JMSException {
        return (Topic) internalDestination;
    }

    public void publish(Message message) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/publish, message=" + message);
        internalProducer.send(message);
    }

    public void publish(Message message, int i, int i1, long l) throws JMSException {
        if (DEBUG)
            System.out.println(toString() + "/publish, message=" + message + ", i=" + i + ", i1=" + i1 + ", l=" + l);
        internalProducer.send(message, i, i1, l);
    }

    public void publish(Topic topic, Message message) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/publish, topic=" + topic + ", message=" + message);
        internalProducer.send(topic, message);
    }

    public void publish(Topic topic, Message message, int i, int i1, long l) throws JMSException {
        if (DEBUG)
            System.out.println(toString() + "/publish, topic=" + topic + ", message=" + message + ", i=" + i + ", i1=" + i1 + ", l=" + l);
        internalProducer.send(topic, message, i, i1, l);
    }

    public String toString() {
        return "/PooledProducer, internalDestination=" + internalDestination;
    }
}
