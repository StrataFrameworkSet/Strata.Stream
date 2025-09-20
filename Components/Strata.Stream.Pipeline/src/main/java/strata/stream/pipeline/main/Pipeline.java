/// ///////////////////////////////////////////////////////////////////////////
// Pipeline.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.main;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.IUnboundedStreamExecutor;
import strata.stream.core.unbounded.IUnboundedStreamSource;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

public
class Pipeline<I>
    implements IPipeline<I>
{
    private final String                    name;
    private final IUnboundedStreamSource<I> source;
    private final IUnboundedStreamExecutor  executor;
    private Optional<IStreamExecution>      execution;
    private final Logger                    logger;

    public
    Pipeline(
        String                    name,
        IUnboundedStreamSource<I> source,
        IUnboundedStreamExecutor  executor)
    {
        this.name = name;
        this.source = source;
        this.executor = executor;
        this.execution = Optional.empty();
        this.logger = LogManager.getLogger(getClass());
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute()
    {
        try
        {
            logger.info("Executing pipeline...");

            return executor.execute();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String
    getName() { return name; }

    @Override
    public IUnboundedStreamSource<I>
    getSource() { return source; }

    @Override
    public IUnboundedStreamExecutor
    getExecutor() { return executor; }

    public static <I> Pipeline<I>
    of(
        String                    name,
        IUnboundedStreamSource<I> source,
        IUnboundedStreamExecutor  executor)
    {
        return new Pipeline<>(name,source,executor);
    }
}

//////////////////////////////////////////////////////////////////////////////
