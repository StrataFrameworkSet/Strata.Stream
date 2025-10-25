//////////////////////////////////////////////////////////////////////////////
// StreamOfStreamsConverter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import strata.foundation.core.collection.ICollection;
import strata.foundation.core.collection.SerializableList;

public
class StreamOfListsConverter<T,K,W extends Window>
    implements
        AllWindowFunction<T,ICollection<T>,W>,
        WindowFunction<T,ICollection<T>,K,W>
{
    @Override
    public void
    apply(
        W                         window,
        Iterable<T>               input,
        Collector<ICollection<T>> output) throws Exception
    {
        output.collect(SerializableList.ofIterable(input));
    }

    @Override
    public void
    apply(
        K                            key,
        W                            window,
        Iterable<T>                  input,
        Collector<ICollection<T>> output) throws Exception
    {
        output.collect(SerializableList.ofIterable(input));
    }
}

//////////////////////////////////////////////////////////////////////////////
