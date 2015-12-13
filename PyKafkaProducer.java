/*
javac -g -d . PyKafkaProducer.java
javah -verbose -jni com.ipinyou.kafka.PyKafkaProducer
*/
package com.ipinyou.kafka;

public class PyKafkaProducer {
    static {
        try {
            //System.load("/libpykafkaproducerjni.so");
            System.load("/data/users/guoliang.cao/kafkaclient/java/"
                    + "libpykafkaproducerjni.so");
        } catch (UnsatisfiedLinkError e) {
            // XXX
            System.err.println(e.toString());
        }
    }

    private String topic;
    private String brokers;
    private String confPath;
    private long handle_;

    public PyKafkaProducer(String topic, String confPath) throws Exception
    {
        if (!initialize(topic, null, confPath)) {
            throw new Exception("PyKafkaProducer init failed");
        }
    }

    public PyKafkaProducer(String topic, String brokers, String confPath)
        throws Exception
    {
        if (!initialize(topic, brokers, confPath)) {
            throw new Exception("PyKafkaProducer init failed");
        }
    }

    public native int produceMessages(String[] payloads, String[] keys);

    protected native boolean initialize(String topic, String brokers,
                                        String confPath);
    public native void dispose();
}
