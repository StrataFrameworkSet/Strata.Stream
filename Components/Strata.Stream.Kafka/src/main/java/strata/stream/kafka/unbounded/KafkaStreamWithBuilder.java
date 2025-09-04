//////////////////////////////////////////////////////////////////////////////
// KafkaStreamWithBuilder.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public
class KafkaStreamWithBuilder<K,T>
{
    private final KStream<K,T>   stream;
    private final StreamsBuilder builder;

    public KafkaStreamWithBuilder(
        KStream<K,T>   s,
        StreamsBuilder b)
    {
        stream    = s;
        builder   = b;
    }

    public KStream<K,T>
    getStream() { return stream; }

    public StreamsBuilder
    getBuilder() { return builder; }

}

//////////////////////////////////////////////////////////////////////////////
