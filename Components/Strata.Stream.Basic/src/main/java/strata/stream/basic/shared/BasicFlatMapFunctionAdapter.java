//////////////////////////////////////////////////////////////////////////////
// BasicFlatMapFunctionAdapter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.shared;

import strata.stream.core.shared.IFunction;

import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public
class BasicFlatMapFunctionAdapter<T,R>
    implements Function<T,Stream<R>>
{
    private final IFunction<T,Iterable<R>> mapper;

    public BasicFlatMapFunctionAdapter(IFunction<T,Iterable<R>> m)
    {
        mapper = m;
    }

    @Override
    public Stream<R>
    apply(T t)
    {
        return
            StreamSupport.stream(
                mapper
                    .apply(t)
                    .spliterator(),
                false);
    }
}

//////////////////////////////////////////////////////////////////////////////
