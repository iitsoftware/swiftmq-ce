package com.swiftmq.extension.amqpbridge.accounting;

import com.swiftmq.extension.amqpbridge.SwiftletContext;
import com.swiftmq.swiftlet.accounting.*;

import java.util.HashMap;
import java.util.Map;

public class AMQPBridgeSourceFactory implements AccountingSourceFactory {
    SwiftletContext ctx = null;
    Map parms = null;

    public AMQPBridgeSourceFactory(SwiftletContext ctx) {
        this.ctx = ctx;
        parms = new HashMap();
        Parameter p = new Parameter("Flush Interval", "Flush Interval of Accounting Data in milliseconds", "60000", false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    long ms = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Bridge Type", "Must be 091 or 100", null, true, new ParameterVerifier() {
            public void verify(Parameter parameter, String s) throws InvalidValueException {
                if (s == null || (!(s.equals("091") || s.equals("100"))))
                    throw new InvalidValueException("Bridge Type must be 091 or 100");
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Bridge Name", "Bridge Name", null, true, null);
        parms.put(p.getName(), p);
    }

    public boolean isSingleton() {
        return false;
    }

    public String getGroup() {
        return "AMQP Bridge";
    }

    public String getName() {
        return "AMQPBridgeSourceFactory";
    }

    public Map getParameters() {
        return parms;
    }

    public AccountingSource create(Map map) throws Exception {
        long flushInterval = Long.parseLong((String) map.get("Flush Interval"));
        String bridgeType = (String) map.get("Bridge Type");
        String bridgeName = (String) map.get("Bridge Name");
        return new AMQPBridgeSource(ctx, flushInterval,
                new AccountingProfile(ctx, bridgeType, bridgeName));

    }
}
