//////////////////////////////////////////////////////////////////////////////
// KafkaFlatMapFunctionAdapter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.function.Function;

public
class KafkaFlatMapFunctionAdapter<T,R>
    implements ValueMapper<T,Iterable<R>>
{
    private final Function<T,Iterable<R>> mapper;

    public KafkaFlatMapFunctionAdapter(Function<T,Iterable<R>> m)
    {
        mapper = m;
    }

    @Override
    public Iterable<R>
    apply(T value)
    {
        return mapper.apply(value);
    }
}

//////////////////////////////////////////////////////////////////////////////
