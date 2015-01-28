package nl.minvenj.nfi.storm.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Describe...
 *
 * @author <a href="mailto:alan.ma@ymail.com">Alan Ma</a>
 * @version 1.0
 */
public class KafkaTopicMapTester {
    public static void main(String[] args) throws IOException,
            InterruptedException {

        List<Thread> threads = new ArrayList<Thread>();

        Properties props = new Properties();
        InputStream in = KafkaTopicMapTester.class.getClassLoader()
                .getResourceAsStream("kafka-config.properties"); // ... (1)
        props.load(in);
        final ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector connector =
                Consumer.createJavaConsumerConnector(config);   // ... (2)

        Map<String, Integer> topicCountMap =
                new HashMap<String, Integer>();  // ... (3)
        topicCountMap.put("max019", 3);
        topicCountMap.put("anotherTopic", 3);

        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreamMap =
                connector.createMessageStreams(topicCountMap); // ... (4)
        for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry :
                messageStreamMap.entrySet()) {
            List<KafkaStream<byte[], byte[]>> streams = entry.getValue();
            for (KafkaStream<byte[], byte[]> stream : streams) {
                final ConsumerIterator<byte[], byte[]> messages = stream.iterator();
                Thread thread = new Thread(                               // ... (5)
                        new Runnable() {
                            @Override
                            public void run() {
                                StringDecoder decoder = new StringDecoder(config.props());
                                while (messages.hasNext()) {                   // ... (6)
                                    MessageAndMetadata<byte[], byte[]>
                                            messageAndMetaData = messages.next();
                                    System.out.println("consumed: " +
                                            "topic=[" +
                                            messageAndMetaData.topic() + "] " +
                                            "message=[" +
                                            decoder.fromBytes(
                                                    messageAndMetaData.message()) + "]");
                                }
                            }
                        });
                thread.start();
                threads.add(thread);
            }
        }

        for (Thread thread : threads) {                                // ... (7)
            thread.join();
        }

        connector.shutdown();                                         // ... (8)
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.86.5:2181");
        props.put("group.id", "max-group");
        props.put("zk.sessiontimeout.ms", "400");
        props.put("fetch.min.bytes", "1");
        props.put("auto.offset.reset", "smallest");
        props.put("zk.synctime.ms", "200");
        props.put("autocommit.interval.ms", "1000");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        return new ConsumerConfig(props);
    }
}
