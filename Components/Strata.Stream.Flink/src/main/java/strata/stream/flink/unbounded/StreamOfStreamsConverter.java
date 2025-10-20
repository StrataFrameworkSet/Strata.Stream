//////////////////////////////////////////////////////////////////////////////
// StreamOfStreamsConverter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import strata.stream.basic.bounded.BasicBoundedStream;
import strata.stream.core.bounded.IBoundedStream;

import java.util.stream.StreamSupport;

public
class StreamOfStreamsConverter<T,K,W extends Window>
    implements
        AllWindowFunction<
            T,
            IBoundedStream<T>,
            W>,
        WindowFunction<
            T,
            IBoundedStream<T>,
            K,
            W>
{
    @Override
    public void
    apply(
        W                            window,
        Iterable<T>                  input,
        Collector<IBoundedStream<T>> output) throws Exception
    {
        output.collect(
            BasicBoundedStream.of(
                StreamSupport.stream(input.spliterator(),false)));
    }

    @Override
    public void
    apply(
        K                            key,
        W                            window,
        Iterable<T>                  input,
        Collector<IBoundedStream<T>> output) throws Exception
    {
        output.collect(
            BasicBoundedStream.of(
                StreamSupport.stream(input.spliterator(),false)));
    }
}

//////////////////////////////////////////////////////////////////////////////
