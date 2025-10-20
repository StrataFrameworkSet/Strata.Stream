//////////////////////////////////////////////////////////////////////////////
// SinkAdapter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import strata.stream.core.unbounded.IUnboundedStreamSink;

import java.io.IOException;

public
class SinkAdapter<T>
    implements Sink<T>
{
    private final IUnboundedStreamSink<T> sink;

    public
    SinkAdapter(IUnboundedStreamSink<T> s)
    {
        sink = s;
    }

    @Override
    public SinkWriter<T>
    createWriter(InitContext context) throws IOException
    {
        return new SinkWriterAdapter<>(sink);
    }
}

//////////////////////////////////////////////////////////////////////////////
