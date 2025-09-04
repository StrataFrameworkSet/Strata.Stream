//////////////////////////////////////////////////////////////////////////////
// ListBoundedStreamSink.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import strata.stream.core.bounded.AbstractBoundedStreamSink;

import java.util.ArrayList;
import java.util.List;

public
class ListBoundedStreamSink<T>
    extends AbstractBoundedStreamSink<T>
{
    private final List<T> store;

    public ListBoundedStreamSink()
    {
        this(new ArrayList<>());
    }

    public ListBoundedStreamSink(List<T> s)
    {
        store = s;
    }

    public ListBoundedStreamSink<T>
    clearStore()
    {
        store.clear();
        return this;
    }

    public List<T>
    getStore() { return store; }

    @Override
    protected void
    process(T element)
    {
        store.add(element);
    }
}

//////////////////////////////////////////////////////////////////////////////
