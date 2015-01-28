package nl.minvenj.nfi.storm.kafka;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Describe...
 *
 * @author <a href="mailto:alan.ma@ymail.com">Alan Ma</a>
 * @version 1.0
 */
public class KafkaWildcardTopicTester {
    public static void main(String[] args) throws IOException,
            InterruptedException {

        List<Thread> threads = new ArrayList<Thread>();

        Properties props = new Properties();
//        InputStream in = new FileInputStream(
//                new File("kafka-config.properties"));
        InputStream in = KafkaWildcardTopicTester.class.getClassLoader()
                .getResourceAsStream("kafka-config.properties"); // ... (1)
        props.load(in);
        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector connector =
                Consumer.createJavaConsumerConnector(config);   // ... (2)

        String regex = "m?\\w+";
        TopicFilter topicFilter = new Whitelist(regex);        // ... (3)

        List<KafkaStream<byte[], byte[]>> streams =
                connector.createMessageStreamsByFilter(topicFilter, 1);       // ... (4)
        for (KafkaStream<byte[], byte[]> stream : streams) {
            final ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
            Thread thread = new Thread(                               // ... (5)
                    new Runnable() {
                        @Override
                        public void run() {
                            StringDecoder decoder = new StringDecoder(new VerifiableProperties());
                            while (iterator.hasNext()) {                   // ... (6)
                                MessageAndMetadata<byte[], byte[]> messageAndMetaData =
                                        iterator.next();
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
