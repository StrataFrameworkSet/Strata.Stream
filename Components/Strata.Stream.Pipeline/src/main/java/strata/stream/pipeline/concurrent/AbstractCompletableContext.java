//////////////////////////////////////////////////////////////////////////////
// AbstractCompletableContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import strata.foundation.core.utility.DefaultIdentifierGenerator;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public abstract
class AbstractCompletableContext<T extends Serializable>
    implements ICompletableContext<T>
{
    private String  id;
    private T       value;
    private boolean filteredOut;

    protected
    AbstractCompletableContext(T value)
    {
        this(
            new DefaultIdentifierGenerator().getNextStringId(16),
            value,
            false);
    }

    protected
    AbstractCompletableContext(T value,ICompletableContext<?> previous)
    {
        this(previous.getId(),value,previous.isFilteredOut());
    }

    protected
    AbstractCompletableContext(String id,T value)
    {
        this(id,value,false);
    }

    protected
    AbstractCompletableContext(String id,T value,boolean filteredOut)
    {
        this.id = id;
        this.value = value;
        this.filteredOut = filteredOut;
    }

    @Override
    public String
    getId() { return id; }

    @Override
    public T
    getValue() { return value; }

    @Override
    public boolean
    hasValue() { return Objects.nonNull(value); }

    @Override
    public boolean
    isFilteredOut() { return filteredOut; }

    @Override
    public ICompletableContext<T>
    filterOut()
    {
        filteredOut = true;
        return this;
    }

    protected void
    setValue(T value)
    {
        this.value = value;
    }
}

//////////////////////////////////////////////////////////////////////////////
