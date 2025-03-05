import com.java.kafka.*;

import java.util.HashMap;
import java.util.Map;

// Acts as the user for the kafka
public class TestKafka {
    public static void main(String[] args) {
        // Simulating the kafka init
        // In real time, these will be done by reading the config files
        KafkaBroker broker = KafkaBroker.getInstance();

        KafkaTopic topic1 = new KafkaTopic(1, "Topic 1", 3);
        KafkaTopic topic2 = new KafkaTopic(2, "Topic 2", 3);

        Map<Integer, KafkaTopic> topicMap = new HashMap<>();
        topicMap.put(1, topic1);
        topicMap.put(2, topic2);

        ConsumerGroup consumerGroup1 = new ConsumerGroup(2);
        ConsumerGroup consumerGroup2 = new ConsumerGroup(2);

        consumerGroup1.subscribe(topic1);
        consumerGroup2.subscribe(topic2);
        consumerGroup1.subscribe(topic2);

        // Message producer logic

        // For custom input, comment the below lines and uncomment the code below that
        broker.produce(new KafkaMessage(5, topic1));
        broker.produce(new KafkaMessage(6, topic2));
        broker.produce(new KafkaMessage(7, topic1));
        broker.produce(new KafkaMessage(8, topic2));
        broker.produce(new KafkaMessage(9, topic1));
        broker.produce(new KafkaMessage(10, topic2));

        // For custom input, uncomment the below lines...

//        Scanner sc = new Scanner(System.in);
//        while(true) {
//            String input = sc.nextLine();
//            if(input.equals("exit")) {
//                break;
//            }
//            String[] tokens = input.split(" ");
//            int value = Integer.parseInt(tokens[0]);
//            int topicId = Integer.parseInt(tokens[1]);
//            com.java.kafka.KafkaTopic topic = topicMap.get(topicId);
//            queue.produce(new com.java.kafka.KafkaMessage(value, topic));
//        }
    }
}
