//////////////////////////////////////////////////////////////////////////////
// StreamOfStreamsConverter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import strata.stream.basic.bounded.BasicBoundedStream;
import strata.stream.core.bounded.IBoundedStream;

import java.util.stream.StreamSupport;

public
class StreamOfStreamsConverter<T,K>
    implements
        AllWindowFunction<
            T,
            IBoundedStream<T>,
            TimeWindow>,
        WindowFunction<
            T,
            IBoundedStream<T>,
            K,
            TimeWindow>
{
    @Override
    public void
    apply(
        TimeWindow                   window,
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
        TimeWindow                   window,
        Iterable<T>                  input,
        Collector<IBoundedStream<T>> output) throws Exception
    {
        output.collect(
            BasicBoundedStream.of(
                StreamSupport.stream(input.spliterator(),false)));
    }
}

//////////////////////////////////////////////////////////////////////////////
