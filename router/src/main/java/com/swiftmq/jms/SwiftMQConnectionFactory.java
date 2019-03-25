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

package com.swiftmq.jms;

import com.swiftmq.client.Versions;
import com.swiftmq.tools.file.GenerationalFileWriter;
import com.swiftmq.tools.file.NumberGenerationProvider;
import com.swiftmq.tools.file.RolloverSizeProvider;

import javax.jms.ConnectionFactory;
import javax.jms.Message;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Writer;
import java.util.Map;

public class SwiftMQConnectionFactory {
    public static final String SOCKETFACTORY = "socketfactory";
    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String KEEPALIVEINTERVAL = "keepaliveinterval";
    public static final String TCP_NO_DELAY = "tcp_no_delay";
    public static final String CLIENTID = "clientid";
    public static final String SMQP_PRODUCER_REPLY_INTERVAL = "smqp_producer_reply_interval";
    public static final String SMQP_CONSUMER_CACHE_SIZE = "smqp_consumer_cache_size";
    public static final String SMQP_CONSUMER_CACHE_SIZE_KB = "smqp_consumer_cache_size_kb";
    public static final String JMS_DELIVERY_MODE = "jms_delivery_mode";
    public static final String JMS_PRIORITY = "jms_priority";
    public static final String JMS_TTL = "jms_ttl";
    public static final String JMS_MESSAGEID_ENABLED = "jms_messageid_enabled";
    public static final String JMS_TIMESTAMP_ENABLED = "jms_timestamp_enabled";
    public static final String USE_THREAD_CONTEXT_CLASSLOADER = "use_thread_context_classloader";
    public static final String INPUT_BUFFER_SIZE = "input_buffer_size";
    public static final String INPUT_EXTEND_SIZE = "input_extend_size";
    public static final String OUTPUT_BUFFER_SIZE = "output_buffer_size";
    public static final String OUTPUT_EXTEND_SIZE = "output_extend_size";
    public static final String INTRAVM = "intravm";
    public static final String RECONNECT_ENABLED = "reconnect_enabled";
    public static final String RECONNECT_RETRY_DELAY = "reconnect_retry_delay";
    public static final String RECONNECT_MAX_RETRIES = "reconnect_max_retries";
    public static final String RECONNECT_HOSTNAME2 = "reconnect_hostname2";
    public static final String RECONNECT_PORT2 = "reconnect_port2";
    public static final String DUPLICATE_DETECTION_ENABLED = "duplicate_detection_enabled";
    public static final String DUPLICATE_BACKLOG_SIZE = "duplicate_backlog_size";

    static {
        if (Boolean.valueOf(System.getProperty("swiftmq.client.debugtofile.enabled", "false")).booleanValue()) {
            System.setOut(new PrintStream(new OutputStream() {
                byte[] buffer = new byte[1];
                Writer writer = null;

                public void write(byte[] bytes) throws IOException {
                    checkWriter();
                    writer.write(new String(bytes));
                }

                public void write(byte[] bytes, int i, int i1) throws IOException {
                    checkWriter();
                    writer.write(new String(bytes, i, i1));
                }

                public void flush() throws IOException {
                    checkWriter();
                    writer.flush();
                }

                public void close() throws IOException {
                    checkWriter();
                    writer.close();
                }

                public void write(int i) throws IOException {
                    checkWriter();
                    buffer[0] = (byte) i;
                    writer.write(new String(buffer));
                }

                private void checkWriter() throws IOException {
                    if (writer == null) {
                        writer = new GenerationalFileWriter(System.getProperty("swiftmq.client.debugtofile.directory", System.getProperty("user.dir")), System.getProperty("swiftmq.client.debugtofile.filename", "swiftmqclient"), new RolloverSizeProvider() {
                            public long getRollOverSize() {
                                return Integer.parseInt(System.getProperty("swiftmq.client.debugtofile.rolloversizekb", "1024")) * 1024;
                            }
                        }, new NumberGenerationProvider() {
                            public int getNumberGenerations() {
                                return Integer.parseInt(System.getProperty("swiftmq.client.debugtofile.generations", "10"));
                            }
                        }
                        );
                    }
                }
            }));
        }
    }

    public SwiftMQConnectionFactory() {
    }

    private static String getMandatoryProp(String prop, Map map) throws Exception {
        String s = (String) map.get(prop);
        if (s == null)
            throw new Exception("Missing mandatory property: " + prop);
        return s;
    }

    private static String getDefaultProp(String prop, Map map, String defaultValue) throws Exception {
        String s = (String) map.get(prop);
        if (s == null)
            s = defaultValue;
        return s;
    }

    public static ConnectionFactory create(Map properties) throws Exception {
        switch (Versions.JMS_CURRENT) {
            case 400:
                return createV400CF(properties);
            case 500:
                return createV500CF(properties);
            case 510:
                return createV510CF(properties);
            case 600:
                return createV600CF(properties);
            case 610:
                return createV610CF(properties);
            case 630:
                return createV630CF(properties);
            case 750:
                return createV750CF(properties);
        }
        return createV750CF(properties);
    }

    private static ConnectionFactory createV400CF(Map properties)
            throws Exception {
        boolean intraVM = false;
        String s = (String) properties.get(INTRAVM);
        if (s != null)
            intraVM = Boolean.valueOf(s).booleanValue();
        com.swiftmq.jms.v400.ConnectionFactoryImpl cf = null;
        if (intraVM) {
            cf = new com.swiftmq.jms.v400.ConnectionFactoryImpl(null,
                    null,
                    null,
                    0,
                    0,
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    true);
        } else {
            cf = new com.swiftmq.jms.v400.ConnectionFactoryImpl(null,
                    getMandatoryProp(SOCKETFACTORY, properties),
                    getMandatoryProp(HOSTNAME, properties),
                    Integer.parseInt(getMandatoryProp(PORT, properties)),
                    Long.parseLong(getDefaultProp(KEEPALIVEINTERVAL, properties, "0")),
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    false);
        }
        return cf;
    }

    private static ConnectionFactory createV500CF(Map properties)
            throws Exception {
        boolean intraVM = false;
        String s = (String) properties.get(INTRAVM);
        if (s != null)
            intraVM = Boolean.valueOf(s).booleanValue();
        com.swiftmq.jms.v500.ConnectionFactoryImpl cf = null;
        if (intraVM) {
            cf = new com.swiftmq.jms.v500.ConnectionFactoryImpl(null,
                    null,
                    null,
                    0,
                    0,
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    true);
        } else {
            cf = new com.swiftmq.jms.v500.ConnectionFactoryImpl(null,
                    getMandatoryProp(SOCKETFACTORY, properties),
                    getMandatoryProp(HOSTNAME, properties),
                    Integer.parseInt(getMandatoryProp(PORT, properties)),
                    Long.parseLong(getDefaultProp(KEEPALIVEINTERVAL, properties, "0")),
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    false);
        }
        return cf;
    }

    private static ConnectionFactory createV510CF(Map properties)
            throws Exception {
        boolean intraVM = false;
        String s = (String) properties.get(INTRAVM);
        if (s != null)
            intraVM = Boolean.valueOf(s).booleanValue();
        com.swiftmq.jms.v510.ConnectionFactoryImpl cf = null;
        if (intraVM) {
            cf = new com.swiftmq.jms.v510.ConnectionFactoryImpl(null,
                    null,
                    null,
                    0,
                    0,
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    true);
        } else {
            cf = new com.swiftmq.jms.v510.ConnectionFactoryImpl(null,
                    getMandatoryProp(SOCKETFACTORY, properties),
                    getMandatoryProp(HOSTNAME, properties),
                    Integer.parseInt(getMandatoryProp(PORT, properties)),
                    Long.parseLong(getDefaultProp(KEEPALIVEINTERVAL, properties, "0")),
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    false);
        }
        return cf;
    }

    private static ConnectionFactory createV600CF(Map properties)
            throws Exception {
        boolean intraVM = false;
        String s = (String) properties.get(INTRAVM);
        if (s != null)
            intraVM = Boolean.valueOf(s).booleanValue();
        com.swiftmq.jms.v600.ConnectionFactoryImpl cf = null;
        if (intraVM) {
            cf = new com.swiftmq.jms.v600.ConnectionFactoryImpl(null,
                    null,
                    null,
                    0,
                    0,
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    true);
        } else {
            cf = new com.swiftmq.jms.v600.ConnectionFactoryImpl(null,
                    getMandatoryProp(SOCKETFACTORY, properties),
                    getMandatoryProp(HOSTNAME, properties),
                    Integer.parseInt(getMandatoryProp(PORT, properties)),
                    Long.parseLong(getDefaultProp(KEEPALIVEINTERVAL, properties, "0")),
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    false);
            cf.setReconnectEnabled(Boolean.valueOf(getDefaultProp(RECONNECT_ENABLED, properties, "false")).booleanValue());
            cf.setRetryDelay(Long.parseLong(getDefaultProp(RECONNECT_RETRY_DELAY, properties, "10000")));
            cf.setMaxRetries(Integer.parseInt(getDefaultProp(RECONNECT_MAX_RETRIES, properties, "10")));
            cf.setHostname2(getDefaultProp(RECONNECT_HOSTNAME2, properties, null));
            cf.setPort2(Integer.parseInt(getDefaultProp(RECONNECT_PORT2, properties, "0")));
            cf.setDuplicateMessageDetection(Boolean.valueOf(getDefaultProp(DUPLICATE_DETECTION_ENABLED, properties, "false")).booleanValue());
            cf.setDuplicateBacklogSize(Integer.parseInt(getDefaultProp(DUPLICATE_BACKLOG_SIZE, properties, "30000")));
        }
        return cf;
    }

    private static ConnectionFactory createV610CF(Map properties)
            throws Exception {
        boolean intraVM = false;
        String s = (String) properties.get(INTRAVM);
        if (s != null)
            intraVM = Boolean.valueOf(s).booleanValue();
        com.swiftmq.jms.v610.ConnectionFactoryImpl cf = null;
        if (intraVM) {
            cf = new com.swiftmq.jms.v610.ConnectionFactoryImpl(null,
                    null,
                    null,
                    0,
                    0,
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    true);
        } else {
            cf = new com.swiftmq.jms.v610.ConnectionFactoryImpl(null,
                    getMandatoryProp(SOCKETFACTORY, properties),
                    getMandatoryProp(HOSTNAME, properties),
                    Integer.parseInt(getMandatoryProp(PORT, properties)),
                    Long.parseLong(getDefaultProp(KEEPALIVEINTERVAL, properties, "0")),
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    false);
            cf.setReconnectEnabled(Boolean.valueOf(getDefaultProp(RECONNECT_ENABLED, properties, "false")).booleanValue());
            cf.setRetryDelay(Long.parseLong(getDefaultProp(RECONNECT_RETRY_DELAY, properties, "10000")));
            cf.setMaxRetries(Integer.parseInt(getDefaultProp(RECONNECT_MAX_RETRIES, properties, "10")));
            cf.setHostname2(getDefaultProp(RECONNECT_HOSTNAME2, properties, null));
            cf.setPort2(Integer.parseInt(getDefaultProp(RECONNECT_PORT2, properties, "0")));
            cf.setDuplicateMessageDetection(Boolean.valueOf(getDefaultProp(DUPLICATE_DETECTION_ENABLED, properties, "false")).booleanValue());
            cf.setDuplicateBacklogSize(Integer.parseInt(getDefaultProp(DUPLICATE_BACKLOG_SIZE, properties, "30000")));
        }
        return cf;
    }

    private static ConnectionFactory createV630CF(Map properties)
            throws Exception {
        boolean intraVM = false;
        String s = (String) properties.get(INTRAVM);
        if (s != null)
            intraVM = Boolean.valueOf(s).booleanValue();
        com.swiftmq.jms.v630.ConnectionFactoryImpl cf = null;
        if (intraVM) {
            cf = new com.swiftmq.jms.v630.ConnectionFactoryImpl(null,
                    null,
                    null,
                    0,
                    0,
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    true);
        } else {
            cf = new com.swiftmq.jms.v630.ConnectionFactoryImpl(null,
                    getMandatoryProp(SOCKETFACTORY, properties),
                    getMandatoryProp(HOSTNAME, properties),
                    Integer.parseInt(getMandatoryProp(PORT, properties)),
                    Long.parseLong(getDefaultProp(KEEPALIVEINTERVAL, properties, "0")),
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    false);
            cf.setReconnectEnabled(Boolean.valueOf(getDefaultProp(RECONNECT_ENABLED, properties, "false")).booleanValue());
            cf.setRetryDelay(Long.parseLong(getDefaultProp(RECONNECT_RETRY_DELAY, properties, "10000")));
            cf.setMaxRetries(Integer.parseInt(getDefaultProp(RECONNECT_MAX_RETRIES, properties, "10")));
            cf.setHostname2(getDefaultProp(RECONNECT_HOSTNAME2, properties, null));
            cf.setPort2(Integer.parseInt(getDefaultProp(RECONNECT_PORT2, properties, "0")));
            cf.setDuplicateMessageDetection(Boolean.valueOf(getDefaultProp(DUPLICATE_DETECTION_ENABLED, properties, "false")).booleanValue());
            cf.setDuplicateBacklogSize(Integer.parseInt(getDefaultProp(DUPLICATE_BACKLOG_SIZE, properties, "30000")));
        }
        return cf;
    }

    private static ConnectionFactory createV750CF(Map properties)
            throws Exception {
        boolean intraVM = false;
        String s = (String) properties.get(INTRAVM);
        if (s != null)
            intraVM = Boolean.valueOf(s).booleanValue();
        com.swiftmq.jms.v750.ConnectionFactoryImpl cf = null;
        if (intraVM) {
            cf = new com.swiftmq.jms.v750.ConnectionFactoryImpl(null,
                    null,
                    null,
                    0,
                    0,
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE_KB, properties, "-1")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    true);
        } else {
            cf = new com.swiftmq.jms.v750.ConnectionFactoryImpl(null,
                    getMandatoryProp(SOCKETFACTORY, properties),
                    getMandatoryProp(HOSTNAME, properties),
                    Integer.parseInt(getMandatoryProp(PORT, properties)),
                    Long.parseLong(getDefaultProp(KEEPALIVEINTERVAL, properties, "0")),
                    getDefaultProp(CLIENTID, properties, null),
                    Integer.parseInt(getDefaultProp(SMQP_PRODUCER_REPLY_INTERVAL, properties, "20")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE, properties, "500")),
                    Integer.parseInt(getDefaultProp(SMQP_CONSUMER_CACHE_SIZE_KB, properties, "-1")),
                    Integer.parseInt(getDefaultProp(JMS_DELIVERY_MODE, properties, String.valueOf(Message.DEFAULT_DELIVERY_MODE))),
                    Integer.parseInt(getDefaultProp(JMS_PRIORITY, properties, String.valueOf(Message.DEFAULT_PRIORITY))),
                    Long.parseLong(getDefaultProp(JMS_TTL, properties, String.valueOf(Message.DEFAULT_TIME_TO_LIVE))),
                    Boolean.valueOf(getDefaultProp(JMS_MESSAGEID_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(JMS_TIMESTAMP_ENABLED, properties, "true")).booleanValue(),
                    Boolean.valueOf(getDefaultProp(USE_THREAD_CONTEXT_CLASSLOADER, properties, "false")).booleanValue(),
                    Integer.parseInt(getDefaultProp(INPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(INPUT_EXTEND_SIZE, properties, "65536")),
                    Integer.parseInt(getDefaultProp(OUTPUT_BUFFER_SIZE, properties, "131072")),
                    Integer.parseInt(getDefaultProp(OUTPUT_EXTEND_SIZE, properties, "65536")),
                    false);
            cf.setReconnectEnabled(Boolean.valueOf(getDefaultProp(RECONNECT_ENABLED, properties, "false")).booleanValue());
            cf.setRetryDelay(Long.parseLong(getDefaultProp(RECONNECT_RETRY_DELAY, properties, "10000")));
            cf.setMaxRetries(Integer.parseInt(getDefaultProp(RECONNECT_MAX_RETRIES, properties, "10")));
            cf.setHostname2(getDefaultProp(RECONNECT_HOSTNAME2, properties, null));
            cf.setPort2(Integer.parseInt(getDefaultProp(RECONNECT_PORT2, properties, "0")));
            cf.setDuplicateMessageDetection(Boolean.valueOf(getDefaultProp(DUPLICATE_DETECTION_ENABLED, properties, "false")).booleanValue());
            cf.setDuplicateBacklogSize(Integer.parseInt(getDefaultProp(DUPLICATE_BACKLOG_SIZE, properties, "30000")));
        }
        return cf;
    }
}
