//////////////////////////////////////////////////////////////////////////////
// KafkaUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public
class KafkaUnboundedStream<K,T>
    extends AbstractKafkaUnboundedStream<K,T>
{
    public
    KafkaUnboundedStream(
        KStream<K,T>   stream,
        StreamsBuilder builder)
    {
        super(stream,builder);
    }

}

//////////////////////////////////////////////////////////////////////////////
