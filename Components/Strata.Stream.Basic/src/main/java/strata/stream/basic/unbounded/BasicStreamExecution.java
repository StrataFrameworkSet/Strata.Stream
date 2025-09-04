/// ///////////////////////////////////////////////////////////////////////////
// BasicStreamExecution.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.unbounded;

import strata.stream.core.shared.IExecutionResult;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.shared.StreamExecutionStatus;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public
class BasicStreamExecution
    implements IStreamExecution
{
    private final IController                      controller;
    private final Supplier<IExecutionResult>       result;
    private final ExecutorService                  executor;
    private AtomicReference<StreamExecutionStatus> status;

    public
    BasicStreamExecution(
        IController                controller,
        Supplier<IExecutionResult> result,
        ExecutorService            executor)
    {
        this.controller = controller;
        this.result = result;
        this.executor = executor;
        this.status = new AtomicReference<>(StreamExecutionStatus.INITIALIZED);
    }

    @Override
    public CompletionStage<Void>
    cancel()
    {
        return CompletableFuture.runAsync(this::doCancel,executor);
    }

    @Override
    public CompletionStage<StreamExecutionStatus>
    getStatus()
    {
        return CompletableFuture.supplyAsync(this::doGetStatus,executor);
    }

    @Override
    public CompletionStage<IExecutionResult>
    getResult()
    {
        return CompletableFuture.supplyAsync(result,executor);
    }

    private void
    doCancel()
    {
        status.set(StreamExecutionStatus.CANCELLING);
        controller.stop();
        status.set(StreamExecutionStatus.CANCELLED);
    }

    private StreamExecutionStatus
    doGetStatus()
    {
        if (controller.isRunning())
            return StreamExecutionStatus.RUNNING;
        else
            return status.get();
    }
}

//////////////////////////////////////////////////////////////////////////////
