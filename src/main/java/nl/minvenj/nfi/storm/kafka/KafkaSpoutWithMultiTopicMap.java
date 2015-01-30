package nl.minvenj.nfi.storm.kafka;

import backtype.storm.spout.Scheme;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;
import org.apache.storm.guava.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provide topic filtering when using High-level Kafka Consumer API.
 *
 * @author <a href="mailto:alan.ma@ymail.com">Alan Ma</a>
 * @version 1.0
 */
public class KafkaSpoutWithMultiTopicMap extends KafkaSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutWithMultiTopicMap.class);
    protected List<ConsumerIterator<byte[], byte[]>> _consumerIterators;
    private Map<String, Integer> topicCountMap;

    /**
     * Creates a new kafka spout to be submitted in a storm topology. Configuration is read from storm config when the
     * spout is opened. Uses a {@link backtype.storm.spout.RawScheme} to serialize messages from kafka as a single {@code byte[]}.
     */
    public KafkaSpoutWithMultiTopicMap() {
        super();
    }

    /**
     * Creates a new kafka spout to be submitted in a storm topology with the provided {@link backtype.storm.spout.Scheme}. This impacts
     * output fields, see {@link #declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)}). Configuration is read from storm config
     * when the spout is opened.
     *
     * @param serializationScheme The serialization to apply to messages read from kafka.
     */
    public KafkaSpoutWithMultiTopicMap(final Scheme serializationScheme) {
        super(serializationScheme);
    }

    /**
     * Creates a new kafka spout to be submitted in a storm topology. Configuration is read from storm config when the
     * spout is opened.
     *
     * @param topic The kafka topic to read messages from.
     */
    public KafkaSpoutWithMultiTopicMap(final String topic) {
        super(topic);
    }

    /**
     * Creates a new kafka spout to be submitted in a storm topology with the provided {@link Scheme}. This impacts
     * output fields, see {@link #declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)}). Configuration is read from storm config
     * when the spout is opened.
     *
     * @param topic               The kafka topic to read messages from.
     * @param serializationScheme The serialization to apply to messages read from kafka.
     */
    public KafkaSpoutWithMultiTopicMap(final String topic, final Scheme serializationScheme) {
        super(topic, serializationScheme);
    }

    /**
     * Refills the buffer with messages from the configured kafka topic if available.
     *
     * @return Whether the buffer contains messages to be emitted after this call.
     * @throws IllegalStateException When current buffer is not empty or messages not acknowledged by topology.
     */
    @Override
    protected boolean fillBuffer() {
        if (!_inProgress.isEmpty() || !_queue.isEmpty()) {
            throw new IllegalStateException("cannot fill buffer when buffer or pending messages are non-empty");
        }

        if (topicCountMap == null || topicCountMap.size() < 1) {
            throw new IllegalStateException("cannot create consumer when topics are not provided");
        }

        if (_consumerIterators == null) {

            _consumerIterators = new ArrayList<ConsumerIterator<byte[], byte[]>>(5);

            // create a stream of messages from _consumer using the streams as defined on construction
            Map<String, List<KafkaStream<byte[], byte[]>>> messageStreamMap =
                    _consumer.createMessageStreams(ImmutableMap.copyOf(topicCountMap));

            for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry :
                    messageStreamMap.entrySet()) {
                List<KafkaStream<byte[], byte[]>> streams = entry.getValue();
                _consumerIterators.addAll(streams.stream().map(KafkaStream<byte[], byte[]>::iterator).collect(Collectors.toList()));
            }
        }

        // We'll iterate the stream in a try-clause; kafka stream will poll its client channel for the next message,
        // throwing a ConsumerTimeoutException when the configured timeout is exceeded.
        for (ConsumerIterator<byte[], byte[]> consumerIterator : _consumerIterators) {

            try {
                int size = 0;
                while (size < _bufSize && consumerIterator.hasNext()) {
                    final MessageAndMetadata<byte[], byte[]> message = consumerIterator.next();
                    final KafkaMessageId id = new KafkaMessageId(message.partition(), message.offset());
                    _inProgress.put(id, message.message());
                    LOG.info(String.format("consuming topic %s [%s]", message.topic(), id.toString()));
                    size++;
                }

            } catch (final ConsumerTimeoutException e) {
                // ignore, storm will call nextTuple again at some point in the near future
                // timeout does *not* mean that no messages were read (state is checked below)
            }
        }

        if (_inProgress.size() > 0) {
            // set _queue to all currently pending kafka message ids
            _queue.addAll(_inProgress.keySet());
            LOG.debug("buffer now has {} messages to be emitted", _queue.size());
            // message(s) appended to buffer
            return true;
        } else {
            // no messages appended to buffer
            return false;
        }
    }

    public void setTopicCountMap(Map<String, Integer> topicCountMap) {
        this.topicCountMap = topicCountMap;
    }
}
