//////////////////////////////////////////////////////////////////////////////
// SinkAdapter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
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
    createWriter(WriterInitContext context) throws IOException
    {
        return new SinkWriterAdapter<>(sink);
    }
}

//////////////////////////////////////////////////////////////////////////////
