package com.java.kafka;

import java.util.HashMap;
import java.util.Map;

public class ConsumerGroup {
    private int noOfConsumers;
    private Map<Integer, KafkaConsumer> consumers;

    public ConsumerGroup(int noOfConsumers) {
        Map<Integer, KafkaConsumer> consumers = new HashMap<>();
        for(int consumerId = 0; consumerId < noOfConsumers; consumerId++) {
            KafkaConsumer consumer = new KafkaConsumer(consumerId);
            consumers.put(consumerId, consumer);
        }

        this.consumers = consumers;
        this.noOfConsumers = noOfConsumers;
    }

    public void addConsumer() {
        KafkaConsumer consumer = new KafkaConsumer(noOfConsumers);
        consumers.put(noOfConsumers, consumer);
        noOfConsumers++;
    }

    public void subscribe(KafkaTopic topic) {
        int noOfPartitions = topic.getNoOfPartitions();
        for(int partitionKey = 0; partitionKey < noOfPartitions; partitionKey++) {
            int consumerId = partitionKey % noOfConsumers; // Need to update the logic for partition <-> consumer mapping
            consumers.get(consumerId).subscribe(topic.getKafkaPartition(partitionKey));
        }
    }
}
