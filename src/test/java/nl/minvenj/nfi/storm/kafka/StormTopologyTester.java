package nl.minvenj.nfi.storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import nl.minvenj.nfi.storm.kafka.util.StringScheme;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Describe...
 *
 * @author <a href="mailto:alan.ma@disney.com">Alan Ma</a>
 * @version 1.0
 */
public class StormTopologyTester {

    public static class PrinterBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println("PrinterBolt output: ");
            System.out.println(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //Kafka producer configuration
        //local kafka
        String kafkaZookeeper = "192.168.86.5:2181";
        String brokerList = "192.168.86.10:9092";
        String groupId = "max-group";
        String timeoutInMs = "100";
        String topics = "max019|1,mickey2015|1";

        Properties props = new Properties();
        props.setProperty("kafka.broker.list", brokerList);
        props.setProperty("kafka.zookeeper.connect", kafkaZookeeper);
        props.setProperty("kafka.consumer.timeout.ms", timeoutInMs);
        props.setProperty("kafka.consumer.group.id", groupId);
        props.setProperty("kafka.topics", topics);

        String kafkaSourceId = "kafkaSource";
        String printerBoltId = "kafkaConsumer";

        //source spout
        KafkaSpoutWithMultiTopicMap kafkaSpout = new KafkaSpoutWithMultiTopicMap("max019", new StringScheme());

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        String[] t0 = topics.split(",");
        for (String s : t0) {
            String[] t = s.trim().split(Pattern.quote("|"));//need to escape pipe: "\\|"
            topicCountMap.put(t[0], Integer.parseInt(t[1]));
        }
        kafkaSpout.setTopicCountMap(topicCountMap);

        builder.setSpout(kafkaSourceId, kafkaSpout, 2);
        builder.setBolt(printerBoltId, new PrinterBolt()).shuffleGrouping(kafkaSourceId);

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("kafka.zookeeper.connect", props.getProperty("kafka.zookeeper.connect"));
        conf.put("kafka.group.id", props.getProperty("kafka.consumer.group.id"));
        conf.put("kafka.consumer.timeout.ms", props.getProperty("kafka.consumer.timeout.ms"));

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka-storm-tester", conf, builder.createTopology());

            Thread.sleep(3000000);

            cluster.shutdown();
        }
    }
}
