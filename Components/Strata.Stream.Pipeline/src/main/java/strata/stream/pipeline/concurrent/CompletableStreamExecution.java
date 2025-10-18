/// ///////////////////////////////////////////////////////////////////////////
// CompletableStreamExecution.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import strata.foundation.core.concurrent.CompletionStageMap;
import strata.stream.core.shared.IExecutionResult;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.shared.StreamExecutionStatus;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public
class CompletableStreamExecution<T extends Serializable>
    implements IStreamExecution
{
    private final IStreamExecution                                  execution;
    private final CompletionStageMap<String,ICompletableContext<T>> pending;

    public
    CompletableStreamExecution(
        IStreamExecution                                  execution,
        CompletionStageMap<String,ICompletableContext<T>> pending)
    {
        this.execution = execution;
        this.pending = pending;
    }

    @Override
    public CompletionStage<Void>
    cancel()
    {
        return execution.cancel();
    }

    @Override
    public CompletionStage<StreamExecutionStatus>
    getStatus()
    {
        return execution.getStatus();
    }

    @Override
    public CompletionStage<IExecutionResult>
    getResult()
    {
        return
            execution
                .getResult()
                .thenApply(
                    result ->
                        CompletableExecutionResult
                            .of(
                                result,
                                pending
                                    .joinAll()
                                    .entrySet()
                                    .stream()
                                    .map(e -> Map.entry(e.getKey(),e.getValue().getValue()))
                                    .collect(Collectors.toMap(Entry::getKey,Entry::getValue))));
    }

    public static <T extends Serializable> IStreamExecution
    of(
        IStreamExecution                                  execution,
        CompletionStageMap<String,ICompletableContext<T>> pending)
    {
        return new CompletableStreamExecution(execution,pending);
    }
}

//////////////////////////////////////////////////////////////////////////////
