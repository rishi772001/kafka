package com.java.kafka;

public class KafkaMessage {
    private KafkaTopic topic;
    private int value; // For now only int is supported

    public KafkaMessage(int value, KafkaTopic topic) {
        this.value = value;
        this.topic = topic;
    }

    public int getValue() {
        return value;
    }

    public KafkaPartition getPartition() {
        int partitionKey = this.value % this.topic.getNoOfPartitions(); // Need to update the logic for partition <-> message mapping
        return this.topic.getKafkaPartition(partitionKey);
    }
}
