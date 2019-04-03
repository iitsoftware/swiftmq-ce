package com.swiftmq.jms.springsupport;

public class ConsumerKey {
    String key = null;

    public ConsumerKey(String destName, String selector, boolean noLocal, String durName) {
        StringBuffer b = new StringBuffer(destName);
        b.append("/");
        b.append(selector == null ? "null" : selector);
        b.append("/");
        b.append(noLocal ? "true" : "false");
        b.append("/");
        b.append(durName == null ? "null" : durName);
        key = b.toString();
    }

    public String getKey() {
        return key;
    }

    public String toString() {
        return key;
    }
}
