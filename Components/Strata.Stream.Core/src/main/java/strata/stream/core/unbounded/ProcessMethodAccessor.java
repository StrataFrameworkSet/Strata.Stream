//////////////////////////////////////////////////////////////////////////////
// StreamSinkProcessAccessor.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.unbounded;

import java.io.Serializable;

public
class ProcessMethodAccessor<T>
    implements Serializable
{
    private final AbstractUnboundedStreamSink<T> sink;

    public
    ProcessMethodAccessor(AbstractUnboundedStreamSink<T> s)
    {
        sink = s;
    }

    public void
    process(T element)
    {
        sink.process(element);
    }
}

//////////////////////////////////////////////////////////////////////////////
