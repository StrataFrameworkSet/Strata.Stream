/// ///////////////////////////////////////////////////////////////////////////
// CompletableUnboundedStreamExecutor.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import strata.foundation.core.concurrent.CompletionStageMap;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.IExecutionDriver;
import strata.stream.core.unbounded.IUnboundedStreamExecutor;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

public
class CompletableUnboundedStreamExecutor<T extends Serializable>
    implements IUnboundedStreamExecutor
{
    private final IUnboundedStreamExecutor                          executor;
    private final CompletionStageMap<String,ICompletableContext<T>> pending;

    public
    CompletableUnboundedStreamExecutor(
        IUnboundedStreamExecutor                          executor,
        CompletionStageMap<String,ICompletableContext<T>> pending)
    {
        this.executor = executor;
        this.pending = pending;
    }
    @Override
    public CompletionStage<IStreamExecution>
    execute() throws Exception
    {
        return
            executor
                .execute()
                .thenApply(
                    execution -> CompletableStreamExecution.of(execution,pending));
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(IExecutionDriver driver) throws Exception
    {
        return
            executor
                .execute(driver)
                .thenApply(
                    execution -> CompletableStreamExecution.of(execution,pending));
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(Properties properties) throws Exception
    {
        return
            executor
                .execute(properties)
                .thenApply(
                    execution -> CompletableStreamExecution.of(execution,pending));
    }

    public static <T extends Serializable> CompletableUnboundedStreamExecutor<T>
    of(
        IUnboundedStreamExecutor                          executor,
        CompletionStageMap<String,ICompletableContext<T>> pending)
    {
        return new CompletableUnboundedStreamExecutor(executor,pending);
    }
}

//////////////////////////////////////////////////////////////////////////////
