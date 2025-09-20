/// ///////////////////////////////////////////////////////////////////////////
// AbstractPipelineFactory.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.main;

import strata.stream.core.unbounded.IUnboundedStreamExecutor;
import strata.stream.core.unbounded.IUnboundedStreamSource;

public abstract
class AbstractPipelineFactory<I>
    implements IPipelineFactory<I>
{
    public IPipeline<I>
    create(Class<I> inputType,String name)
    {
        IUnboundedStreamSource<I> source   = getSource(inputType);
        IUnboundedStreamExecutor  executor = configure(source);

        return Pipeline.of(name,source,executor);
    }

    protected abstract IUnboundedStreamSource<I>
    getSource(Class<I> inputType);

    protected abstract IUnboundedStreamExecutor
    configure(IUnboundedStreamSource<I> source);
}

//////////////////////////////////////////////////////////////////////////////
