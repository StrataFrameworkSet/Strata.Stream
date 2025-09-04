//////////////////////////////////////////////////////////////////////////////
// ListAggregator.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public
class ListAggregator<K,T>
    implements Aggregator<K,T,List<T>>
{
    private final Logger logger;

    public
    ListAggregator()
    {
        logger = LogManager.getLogger(getClass());
    }

    @Override
    public List<T>
    apply(K key,T value,List<T> aggregate)
    {
        logger.debug("apply({},{},{})", key, value, aggregate);
        aggregate.add(value);
        return aggregate;
    }
}

//////////////////////////////////////////////////////////////////////////////
