/// ///////////////////////////////////////////////////////////////////////////
// BasicExecutableUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.unbounded;

import strata.foundation.core.concurrent.IBlockingQueue;
import strata.stream.core.shared.IExecutionResult;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.AbstractExecutableUnboundedStream;
import strata.stream.core.unbounded.IExecutionDriver;
import strata.stream.core.unbounded.IUnboundedStream;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public
class BasicExecutableUnboundedStream<T>
    extends AbstractExecutableUnboundedStream<T>
{
    private final IUnboundedStream<T> stream;
    private final IController         controller;
    private final ExecutorService     executor;

    public
    BasicExecutableUnboundedStream(
        IUnboundedStream<T> stream,
        IController controller,
        ExecutorService executor)
    {
        this.stream = stream;
        this.controller = controller;
        this.executor = executor;
    }

    @Override
    protected IUnboundedStream<T>
    getStream()
    {
        return stream;
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute() throws Exception
    {
        return null;
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(IExecutionDriver driver) throws Exception
    {
        return null;
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(Properties properties) throws Exception
    {
        return execute();
    }

    private IStreamExecution
    doExecute()
    {
        return new BasicStreamExecution(controller,() -> doGetResult(controller),executor);
    }

    private IExecutionResult
    doGetResult(IController controller)
    {
        Instant start  = Instant.now();

        controller.start();
        while (controller.isRunning());

        return
            new BasicExecutionResult(
                UUID.randomUUID().toString(),
                Duration.between(start,Instant.now()));
    }
}

//////////////////////////////////////////////////////////////////////////////
