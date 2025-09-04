//////////////////////////////////////////////////////////////////////////////
// BoundedStreamMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.stream.basic.bounded.BasicBoundedStream;
import strata.stream.core.bounded.IBoundedStream;

import java.util.List;

public
class BoundedStreamMapper<K,T>
    implements KeyValueMapper<Windowed<K>,List<T>,KeyValue<K,IBoundedStream<T>>>
{
    private final Logger logger;

    public
    BoundedStreamMapper()
    {
        logger = LogManager.getLogger(getClass());
    }

    @Override
    public KeyValue<K,IBoundedStream<T>>
    apply(Windowed<K> key,List<T> value)
    {
        logger.debug("apply({},{})", key, value);
        return KeyValue.pair(key.key(),BasicBoundedStream.of(value));
    }
}

//////////////////////////////////////////////////////////////////////////////
