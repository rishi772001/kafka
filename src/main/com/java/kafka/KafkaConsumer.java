package com.java.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumer {
    public static final int MESSAGE_CHECK_INTERVAL_MILLIS = 50;

    private int id;
    Map<KafkaPartition, Integer> partitionOffsetMap;

    public KafkaConsumer(int id) {
        this.id = id;
        this.partitionOffsetMap = new HashMap<>();
        startConsumer();
    }

    public void subscribe(KafkaPartition kafkaPartition) {
        partitionOffsetMap.put(kafkaPartition, 0); // Initially setting offset to zero
    }

    // Private helpers

    private void startConsumer() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.submit(() -> {
            try {
                fetchMessages();
            } catch (InterruptedException e) {
                System.out.println("Exception occurred in consumer, Killing the process" + e.getMessage());
                throw new RuntimeException("Kafka consumer action failed in a unexpected way");
            }
        });
    }

    private void fetchMessages() throws InterruptedException {
        try {
            while (true) {
                for (KafkaPartition partition : partitionOffsetMap.keySet()) {
                    int currentProcessingOffset = partitionOffsetMap.get(partition);
                    KafkaMessage message = partition.getMessage(currentProcessingOffset);
                    if (message != null) {
                        try {
                            consume(message);
                            partitionOffsetMap.put(partition, currentProcessingOffset + 1);
                        } catch (KafkaException e) {
                            // Not incrementing the offset for retry, TODO:: Add dead letter queue impl here after threshold number of retries
                            // For now retrying infinite no of times
                            System.out.println("Exception occurred while consuming message");
                        }
                    } else {
                        // This will occur if queue is empty (or) all messages in queue is processed (or) the message is yet to be consumed
                        Thread.sleep(MESSAGE_CHECK_INTERVAL_MILLIS);
                    }

                }
            }
        } catch (Exception e) {
            System.out.println("Consumer failed due to - " + Arrays.toString(e.getStackTrace()));
            throw new InterruptedException("Consumer failed");
        }
    }

    public void consume(KafkaMessage message) throws InterruptedException, KafkaException {
        processMessage(message);
    }

    // Message consumer logic
    // In practice this method will be running in a separate server.
    private static void processMessage(KafkaMessage message) throws InterruptedException, KafkaException {
        Thread.sleep(100); // Imagining if this task takes 100 millis to complete
        System.out.println("Message consumed - " + message.getValue());
    }
}
