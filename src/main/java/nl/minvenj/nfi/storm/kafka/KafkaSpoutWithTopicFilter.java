package nl.minvenj.nfi.storm.kafka;

import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import nl.minvenj.nfi.storm.kafka.util.KafkaMessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Provide topic filtering when using High-level Kafka Consumer API.
 *
 * @author <a href="mailto:alan.ma@ymail.com">Alan Ma</a>
 * @version 1.0
 */
public class KafkaSpoutWithTopicFilter extends KafkaSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutWithTopicFilter.class);

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

        if (_consumerIterator == null) {
            // create a stream of messages from _consumer using the streams as defined on construction
            final Map<String, List<KafkaStream<byte[], byte[]>>> streams = _consumer.createMessageStreams(Collections.singletonMap(_topic, 1));
            _consumerIterator = streams.get(_topic).get(0).iterator();
        }

        // We'll iterate the stream in a try-clause; kafka stream will poll its client channel for the next message,
        // throwing a ConsumerTimeoutException when the configured timeout is exceeded.
        try {
            int size = 0;
            while (size < _bufSize && _consumerIterator.hasNext()) {
                final MessageAndMetadata<byte[], byte[]> message = _consumerIterator.next();
                final KafkaMessageId id = new KafkaMessageId(message.partition(), message.offset());
                _inProgress.put(id, message.message());
                size++;
            }
        }
        catch (final ConsumerTimeoutException e) {
            // ignore, storm will call nextTuple again at some point in the near future
            // timeout does *not* mean that no messages were read (state is checked below)
        }

        if (_inProgress.size() > 0) {
            // set _queue to all currently pending kafka message ids
            _queue.addAll(_inProgress.keySet());
            LOG.debug("buffer now has {} messages to be emitted", _queue.size());
            // message(s) appended to buffer
            return true;
        }
        else {
            // no messages appended to buffer
            return false;
        }
    }

}
