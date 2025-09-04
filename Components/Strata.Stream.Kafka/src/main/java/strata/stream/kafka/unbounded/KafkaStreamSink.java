//////////////////////////////////////////////////////////////////////////////
// KafkaStreamSink.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import strata.stream.core.shared.IStream;

import java.io.Serializable;
import java.util.function.Consumer;

public
class KafkaStreamSink<K,T>
    implements Consumer<IStream<T>>, Serializable
{
    private final String topic;

    public
    KafkaStreamSink(String top)
    {
        topic = top;
    }

    @Override
    public void
    accept(IStream<T> stream)
    {
        if (stream instanceof KafkaUnboundedStream<?,?> kafkaStream)
            kafkaStream
                .getImplementation()
                .to(topic);
        else
            throw new IllegalArgumentException(
                "Only supports KafkaUnboundedStream<K,T> type. " +
                "input was unsupported type: " +
                stream.getClass().getCanonicalName());
    }
}

//////////////////////////////////////////////////////////////////////////////
