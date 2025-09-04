//////////////////////////////////////////////////////////////////////////////
// SinkWriterAdapter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.connector.sink2.SinkWriter;
import strata.stream.core.unbounded.AbstractUnboundedStreamSink;
import strata.stream.core.unbounded.IUnboundedStreamSink;
import strata.stream.core.unbounded.ProcessMethodAccessor;

import java.io.IOException;

public
class SinkWriterAdapter<T>
    implements SinkWriter<T>
{
    private final ProcessMethodAccessor<T> accessor;

    public
    SinkWriterAdapter(IUnboundedStreamSink<T> sink)
    {
        accessor =
            new ProcessMethodAccessor<>((AbstractUnboundedStreamSink<T>)sink);
    }

    @Override
    public void
    write(T element,Context context) throws IOException, InterruptedException
    {
        accessor.process(element);
    }

    @Override
    public void
    flush(boolean endOfInput) throws IOException, InterruptedException
    {
    }

    @Override
    public void
    close() throws Exception
    {

    }
}

//////////////////////////////////////////////////////////////////////////////
