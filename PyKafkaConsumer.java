/*
javac -g -d . IConsumeCallback.java
javac -g -d . BaseConsumeCallback.java
javac -g -d . PyKafkaConsumer.java
javah -verbose -jni com.ipinyou.kafka.PyKafkaConsumer
*/
package com.ipinyou.kafka;

public class PyKafkaConsumer {
    static {
        try {
            //System.load("/libpykafkaproducerjni.so");
            System.load("/data/users/guoliang.cao/kafkaclient/java/"
                    + "libpykafkaconsumerjni.so");
        } catch (UnsatisfiedLinkError e) {
            // XXX
            System.err.println(e.toString());
        }
    }

    private String topic;
    private String brokers;
    private String confPath;
    private long handle_;

    public PyKafkaConsumer(String topic, String confPath) throws Exception
    {
        if (!initialize(topic, null, confPath)) {
            throw new Exception("PyKafkaConsumer init failed");
        }
    }

    public PyKafkaConsumer(String topic, String brokers, String confPath)
        throws Exception
    {
        if (!initialize(topic, brokers, confPath)) {
            throw new Exception("PyKafkaConsumer init failed");
        }
    }

    public native int consumeMessages(int partition, long offset,
                                      IConsumeCallback callback);
                                      //BaseConsumeCallback callback);

    protected native boolean initialize(String topic, String brokers,
                                        String confPath);
    public native void dispose();
}
