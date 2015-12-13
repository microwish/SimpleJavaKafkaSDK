package com.ipinyou.kafka;

public abstract class BaseConsumeCallback {
    public int consume(String message)
    {
        System.out.println("consuming: " + message);
        return 0;
    }
}
