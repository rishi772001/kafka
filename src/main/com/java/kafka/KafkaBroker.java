package com.java.kafka;

public class KafkaBroker {
    private static KafkaBroker INSTANCE;

    // Singleton class - Only one instance per server
    public static KafkaBroker getInstance() {
        if(INSTANCE != null) {
            return INSTANCE;
        }
        INSTANCE = new KafkaBroker();
        return INSTANCE;
    }

    public boolean produce(KafkaMessage message) {
        return message.getPartition().produce(message);
    }
}
