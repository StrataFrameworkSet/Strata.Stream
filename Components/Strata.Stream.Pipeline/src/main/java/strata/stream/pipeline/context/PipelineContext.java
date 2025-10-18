//////////////////////////////////////////////////////////////////////////////
// PipelineContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.stream.core.shared.IFunction;
import strata.stream.pipeline.concurrent.ICompletableContext;

import java.io.Serializable;

public
class PipelineContext<T extends Serializable>
    extends AbstractPipelineContext<T>
{
    public
    PipelineContext(T value)
    {
        super("PipelineStep",value);
    }

    public
    PipelineContext(T value,IPipelineContext<?> previous)
    {
        super(previous.getStepPrefix(),value,previous);
    }

    public
    PipelineContext(String name, T value)
    {
        super(name,value);
    }

    public
    PipelineContext(String name)
    {
        super(name);
    }

    @Override
    public <R extends Serializable> ICompletableContext<R>
    mapValue(IFunction<T,R> mapper)
    {
        return new PipelineContext<>(mapper.apply(getValue()),this);
    }

    public static <T extends Serializable> IPipelineContext<T>
    of(T value)
    {
        return new PipelineContext<>(value);
    }

    public static <T extends Serializable> IPipelineContext<T>
    of(String stepPrefix,T value)
    {
        return new PipelineContext<>(stepPrefix,value);
    }

    public static <T extends Serializable> IPipelineContext<T>
    of(T value,IPipelineContext<?> previous)
    {
        return new PipelineContext<>(value,previous);
    }
}

//////////////////////////////////////////////////////////////////////////////
