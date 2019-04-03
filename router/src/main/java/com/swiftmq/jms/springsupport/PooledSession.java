package com.swiftmq.jms.springsupport;

import javax.jms.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PooledSession
        implements Session, QueueSession, TopicSession {
    static final boolean DEBUG = Boolean.valueOf(System.getProperty("swiftmq.springsupport.debug", "false")).booleanValue();
    private static final String NULL_DESTINATION = "_NULL_";
    SharedJMSConnection internalConnection = null;
    Session internalSession = null;
    Map consumerPool = new HashMap();
    Map producerPool = new HashMap();

    public PooledSession(SharedJMSConnection internalConnection, Session internalSession) {
        this.internalConnection = internalConnection;
        this.internalSession = internalSession;
        if (DEBUG) System.out.println(toString() + "/created");
    }

    protected synchronized void checkIn(PooledConsumer pooledConsumer) {
        if (DEBUG) System.out.println(toString() + "/checkIn, pooledConsumer=" + pooledConsumer);
        consumerPool.put(pooledConsumer.getKey().getKey(), pooledConsumer);
    }

    protected synchronized void checkIn(PooledProducer pooledProducer) {
        if (DEBUG) System.out.println(toString() + "/checkIn, pooledProducer=" + pooledProducer);
        try {
            Destination dest = pooledProducer.getDestination();
            String name = dest != null ? dest.toString() : NULL_DESTINATION;
            producerPool.put(name, pooledProducer);
        } catch (JMSException e) {
        }
    }

    public synchronized void checkExpired() {
        if (consumerPool.size() == 0 && producerPool.size() == 0)
            return;
        for (Iterator iter = consumerPool.entrySet().iterator(); iter.hasNext(); ) {
            PooledConsumer pc = (PooledConsumer) ((Map.Entry) iter.next()).getValue();
            if (pc.getCheckInTime() + internalConnection.getPoolExpiration() <= System.currentTimeMillis()) {
                if (DEBUG) System.out.println(toString() + "/checkExpired, expired=" + pc);
                pc.closeInternal();
                iter.remove();
            }
        }
        for (Iterator iter = producerPool.entrySet().iterator(); iter.hasNext(); ) {
            PooledProducer pp = (PooledProducer) ((Map.Entry) iter.next()).getValue();
            if (pp.getCheckInTime() + internalConnection.getPoolExpiration() <= System.currentTimeMillis()) {
                if (DEBUG) System.out.println(toString() + "/checkExpired, expired=" + pp);
                pp.closeInternal();
                iter.remove();
            }
        }
    }

    public BytesMessage createBytesMessage() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createBytesMessage");
        return internalSession.createBytesMessage();
    }

    public MapMessage createMapMessage() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createMapMessage");
        return internalSession.createMapMessage();
    }

    public Message createMessage() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createMessage");
        return internalSession.createMessage();
    }

    public ObjectMessage createObjectMessage() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createObjectMessage");
        return internalSession.createObjectMessage();
    }

    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createObjectMessage2");
        return internalSession.createObjectMessage(object);
    }

    public StreamMessage createStreamMessage() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createStreamMessage");
        return internalSession.createStreamMessage();
    }

    public TextMessage createTextMessage() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createTextMessage");
        return internalSession.createTextMessage();
    }

    public TextMessage createTextMessage(String s) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createTextMessage2");
        return internalSession.createTextMessage(s);
    }

    public boolean getTransacted() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/getTransacted");
        return internalSession.getTransacted();
    }

    public int getAcknowledgeMode() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/getAcknowledgeMode");
        return internalSession.getAcknowledgeMode();
    }

    public void commit() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/commit");
        internalSession.commit();
    }

    public void rollback() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/rollback");
        internalSession.rollback();
    }

    public void close() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/close");
        internalConnection.checkIn(this);
    }

    protected void closeInternal() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/closeInternal");
        for (Iterator iter = consumerPool.entrySet().iterator(); iter.hasNext(); ) {
            PooledConsumer pc = (PooledConsumer) ((Map.Entry) iter.next()).getValue();
            if (DEBUG) System.out.println(toString() + "/closeInternal, close=" + pc);
            pc.closeInternal();
            iter.remove();
        }
        for (Iterator iter = producerPool.entrySet().iterator(); iter.hasNext(); ) {
            PooledProducer pp = (PooledProducer) ((Map.Entry) iter.next()).getValue();
            if (DEBUG) System.out.println(toString() + "/closeInternal, close=" + pp);
            pp.closeInternal();
            iter.remove();
        }
        internalSession.close();
    }

    public void recover() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/recover");
        internalSession.recover();
    }

    public MessageListener getMessageListener() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/getMessageListener");
        return internalSession.getMessageListener();
    }

    public void setMessageListener(MessageListener ml) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/setMessageListener, ml=" + ml);
        internalSession.setMessageListener(ml);
    }

    public void run() {
        if (DEBUG) System.out.println(toString() + "/run");
        internalSession.run();
    }

    public Queue createQueue(String s) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createQueue, s=" + s);
        return internalSession.createQueue(s);
    }

    public Topic createTopic(String s) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createTopic, s=" + s);
        return internalSession.createTopic(s);
    }

    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createBrowser, queue=" + queue);
        return internalSession.createBrowser(queue);
    }

    public QueueBrowser createBrowser(Queue queue, String string) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createBrowser, queue=" + queue + ", sel=" + string);
        return internalSession.createBrowser(queue, string);
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createTemporaryQueue");
        return internalSession.createTemporaryQueue();
    }

    public TemporaryTopic createTemporaryTopic() throws JMSException {
        if (DEBUG) System.out.println(toString() + "/createTemporaryTopic");
        return internalSession.createTemporaryTopic();
    }

    public void unsubscribe(String string) throws JMSException {
        if (DEBUG) System.out.println(toString() + "/unsubscribe, s=" + string);
        internalSession.unsubscribe(string);
    }

    public synchronized QueueSender createSender(Queue queue) throws JMSException {
        String name = queue != null ? queue.toString() : NULL_DESTINATION;
        PooledProducer pp = (PooledProducer) producerPool.remove(name);
        if (pp != null) {
            if (DEBUG) System.out.println(toString() + "/createSender, queue=" + name + ", return from pool");
            return pp;
        }
        if (DEBUG) System.out.println(toString() + "/createSender, queue=" + name + ", creating new");
        return new PooledProducer(this, internalSession.createProducer(queue), queue);
    }

    public synchronized TopicPublisher createPublisher(Topic topic) throws JMSException {
        String name = topic != null ? topic.toString() : NULL_DESTINATION;
        PooledProducer pp = (PooledProducer) producerPool.remove(name);
        if (pp != null) {
            if (DEBUG) System.out.println(toString() + "/createPublisher, topic=" + name + ", return from pool");
            return pp;
        }
        if (DEBUG) System.out.println(toString() + "/createPublisher, topic=" + name + ", creating new");
        return new PooledProducer(this, internalSession.createProducer(topic), topic);
    }

    public synchronized MessageProducer createProducer(Destination destination) throws JMSException {
        String name = destination != null ? destination.toString() : NULL_DESTINATION;
        PooledProducer pp = (PooledProducer) producerPool.remove(name);
        if (pp != null) {
            if (DEBUG) System.out.println(toString() + "/createProducer, destination=" + name + ", return from pool");
            return pp;
        }
        if (DEBUG) System.out.println(toString() + "/createProducer, destination=" + name + ", creating new");
        return new PooledProducer(this, internalSession.createProducer(destination), destination);
    }

    public synchronized QueueReceiver createReceiver(Queue queue) throws JMSException {
        ConsumerKey key = new ConsumerKey(queue.getQueueName(), null, true, null);
        PooledConsumer pc = (PooledConsumer) consumerPool.remove(key.getKey());
        if (pc != null) {
            if (DEBUG) System.out.println(toString() + "/createReceiver, key=" + key + ", return from pool");
            return pc;
        }
        if (DEBUG) System.out.println(toString() + "/createReceiver, key=" + key + ", creating new");
        return new PooledConsumer(this, internalSession.createConsumer(queue), queue, true, key);
    }

    public synchronized QueueReceiver createReceiver(Queue queue, String s) throws JMSException {
        ConsumerKey key = new ConsumerKey(queue.getQueueName(), s, true, null);
        PooledConsumer pc = (PooledConsumer) consumerPool.remove(key.getKey());
        if (pc != null) {
            if (DEBUG) System.out.println(toString() + "/createReceiver, key=" + key + ", return from pool");
            return pc;
        }
        if (DEBUG) System.out.println(toString() + "/createReceiver, key=" + key + ", creating new");
        return new PooledConsumer(this, internalSession.createConsumer(queue, s), queue, true, key);
    }

    public synchronized TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        ConsumerKey key = new ConsumerKey(topic.getTopicName(), null, true, null);
        PooledConsumer pc = (PooledConsumer) consumerPool.remove(key.getKey());
        if (pc != null) {
            if (DEBUG) System.out.println(toString() + "/createSubscriber, key=" + key + ", return from pool");
            return pc;
        }
        if (DEBUG) System.out.println(toString() + "/createSubscriber, key=" + key + ", creating new");
        return new PooledConsumer(this, internalSession.createConsumer(topic), topic, true, key);
    }

    public synchronized TopicSubscriber createSubscriber(Topic topic, String s, boolean b) throws JMSException {
        ConsumerKey key = new ConsumerKey(topic.getTopicName(), s, b, null);
        PooledConsumer pc = (PooledConsumer) consumerPool.remove(key.getKey());
        if (pc != null) {
            if (DEBUG) System.out.println(toString() + "/createSubscriber, key=" + key + ", return from pool");
            return pc;
        }
        if (DEBUG) System.out.println(toString() + "/createSubscriber, key=" + key + ", creating new");
        return new PooledConsumer(this, internalSession.createConsumer(topic, s, b), topic, b, key);
    }

    public synchronized TopicSubscriber createDurableSubscriber(Topic topic, String s) throws JMSException {
        ConsumerKey key = new ConsumerKey(topic.getTopicName(), null, true, s);
        PooledConsumer pc = (PooledConsumer) consumerPool.remove(key.getKey());
        if (pc != null) {
            if (DEBUG) System.out.println(toString() + "/createDurableSubscriber, key=" + key + ", return from pool");
            return pc;
        }
        if (DEBUG) System.out.println(toString() + "/createDurableSubscriber, key=" + key + ", creating new");
        return new PooledConsumer(this, internalSession.createDurableSubscriber(topic, s), topic, true, key);
    }

    public synchronized TopicSubscriber createDurableSubscriber(Topic topic, String s, String s1, boolean b) throws JMSException {
        ConsumerKey key = new ConsumerKey(topic.getTopicName(), s1, b, s);
        PooledConsumer pc = (PooledConsumer) consumerPool.remove(key.getKey());
        if (pc != null) {
            if (DEBUG) System.out.println(toString() + "/createDurableSubscriber, key=" + key + ", return from pool");
            return pc;
        }
        if (DEBUG) System.out.println(toString() + "/createDurableSubscriber, key=" + key + ", creating new");
        return new PooledConsumer(this, internalSession.createDurableSubscriber(topic, s, s1, b), topic, b, key);
    }

    public synchronized MessageConsumer createConsumer(Destination destination) throws JMSException {
        ConsumerKey key = new ConsumerKey(destination.toString(), null, true, null);
        PooledConsumer pc = (PooledConsumer) consumerPool.remove(key.getKey());
        if (pc != null) {
            if (DEBUG) System.out.println(toString() + "/createConsumer, key=" + key + ", return from pool");
            return pc;
        }
        if (DEBUG) System.out.println(toString() + "/createConsumer, key=" + key + ", creating new");
        return new PooledConsumer(this, internalSession.createConsumer(destination), destination, true, key);
    }

    public synchronized MessageConsumer createConsumer(Destination destination, String s) throws JMSException {
        ConsumerKey key = new ConsumerKey(destination.toString(), s, true, null);
        PooledConsumer pc = (PooledConsumer) consumerPool.remove(key.getKey());
        if (pc != null) {
            if (DEBUG) System.out.println(toString() + "/createConsumer, key=" + key + ", return from pool");
            return pc;
        }
        if (DEBUG) System.out.println(toString() + "/createConsumer, key=" + key + ", creating new");
        return new PooledConsumer(this, internalSession.createConsumer(destination, s), destination, true, key);
    }

    public synchronized MessageConsumer createConsumer(Destination destination, String s, boolean b) throws JMSException {
        ConsumerKey key = new ConsumerKey(destination.toString(), s, b, null);
        PooledConsumer pc = (PooledConsumer) consumerPool.remove(key.getKey());
        if (pc != null) {
            if (DEBUG) System.out.println(toString() + "/createConsumer, key=" + key + ", return from pool");
            return pc;
        }
        if (DEBUG) System.out.println(toString() + "/createConsumer, key=" + key + ", creating new");
        return new PooledConsumer(this, internalSession.createConsumer(destination, s, b), destination, b, key);
    }

    public String toString() {
        return "/PooledSession, internalSession=" + internalSession;
    }
}
