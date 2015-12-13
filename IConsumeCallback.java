package com.ipinyou.kafka;

public interface IConsumeCallback {
    public int consume(String message);
}
