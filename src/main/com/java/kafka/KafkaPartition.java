package com.java.kafka;

import java.util.ArrayList;
import java.util.List;

public class KafkaPartition {
    private static final int QUEUE_SIZE = 1000;

    private final int partitionId;
    private List<KafkaMessage> messageQueue;

    public KafkaPartition(int partitionId) {
        this.partitionId = partitionId;
        messageQueue = new ArrayList<>();
    }

    public boolean produce(KafkaMessage message) {
        if(messageQueue.size() >= QUEUE_SIZE) {
            throw new IllegalStateException("Queue is full");
        }
        return messageQueue.add(message);
    }

    public KafkaMessage getMessage(int offset) {
        if(offset >= messageQueue.size()) {
            return null;
        }
        return messageQueue.get(offset);
    }
}
