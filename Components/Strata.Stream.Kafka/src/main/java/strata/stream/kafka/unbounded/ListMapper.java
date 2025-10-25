//////////////////////////////////////////////////////////////////////////////
// BoundedStreamMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.foundation.core.collection.ICollection;
import strata.foundation.core.collection.SerializableList;

import java.util.List;

public
class ListMapper<K,T>
    implements KeyValueMapper<Windowed<K>,List<T>,KeyValue<K,ICollection<T>>>
{
    private final Logger logger;

    public ListMapper()
    {
        logger = LogManager.getLogger(getClass());
    }

    @Override
    public KeyValue<K,ICollection<T>>
    apply(Windowed<K> key,List<T> value)
    {
        logger.debug("apply({},{})", key.key(), value);
        return KeyValue.pair(key.key(),SerializableList.of(value));
    }
}

//////////////////////////////////////////////////////////////////////////////
