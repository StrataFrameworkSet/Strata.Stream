//////////////////////////////////////////////////////////////////////////////
// SetBoundedStreamSink.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import strata.stream.core.bounded.AbstractBoundedStreamSink;

import java.util.*;

public
class SetBoundedStreamSink<T>
    extends AbstractBoundedStreamSink<T>
{
    private final Set<T> store;

    public SetBoundedStreamSink()
    {
        this(new TreeSet<>());
    }

    public SetBoundedStreamSink(Set<T> s)
    {
        store = s;
    }

    public SetBoundedStreamSink<T>
    clearStore()
    {
        store.clear();
        return this;
    }

    public Set<T>
    getStore() { return store; }

    @Override
    protected void
    process(T element)
    {
        store.add(element);
    }
}

//////////////////////////////////////////////////////////////////////////////
