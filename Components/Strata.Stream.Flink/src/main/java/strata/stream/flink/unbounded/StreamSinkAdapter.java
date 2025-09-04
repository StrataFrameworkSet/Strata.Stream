//////////////////////////////////////////////////////////////////////////////
// StreamSinkAdapter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import strata.stream.core.shared.IStream;

public
class StreamSinkAdapter<T>
    implements java.util.function.Consumer<IStream<T>>, java.io.Serializable
{
    private final DataStreamSink<T> sink;

    public
    StreamSinkAdapter(DataStreamSink<T> s)
    {
        sink = s;
    }

    @Override
    public void
    accept(IStream<T> stream) {}
}

//////////////////////////////////////////////////////////////////////////////
