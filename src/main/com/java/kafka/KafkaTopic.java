package com.java.kafka;

import java.util.HashMap;
import java.util.Map;

public class KafkaTopic {
    private int noOfPartitions;
    private int id;
    private String name;
    private Map<Integer, KafkaPartition> partitionMap;

    public KafkaTopic(int id, String name, int noOfPartitions) {
        this.id = id;
        this.name = name;
        this.noOfPartitions = noOfPartitions;
        init(noOfPartitions);
    }

    public int getNoOfPartitions() {
        return noOfPartitions;
    }

    public KafkaPartition getKafkaPartition(int partitionKey) {
        return partitionMap.get(partitionKey);
    }

    // Private helpers
    private void init(int noOfPartitions) {
        HashMap<Integer, KafkaPartition> partitionMap = new HashMap<>();
        for(int i = 0; i < noOfPartitions; i++) {
            partitionMap.put(i, new KafkaPartition(i));
        }

        this.partitionMap = partitionMap;
    }
}
