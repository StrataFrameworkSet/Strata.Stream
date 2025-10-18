/// ///////////////////////////////////////////////////////////////////////////
// CompletableContextHistoryLogger.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.unbounded;

import strata.foundation.core.concurrent.CompletionStageMap;
import strata.stream.core.shared.IConsumer;
import strata.stream.pipeline.concurrent.CompletableStreamStage;
import strata.stream.pipeline.concurrent.CompletableUnboundedStreamSink;
import strata.stream.pipeline.concurrent.ICompletableContext;
import strata.stream.pipeline.context.IPipelineContext;
import strata.stream.pipeline.context.PipelineStep;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public
class CompletableContextHistorySink
    extends CompletableUnboundedStreamSink<String,String>
{
    public
    CompletableContextHistorySink(
        CompletionStageMap<String,ICompletableContext<String>> pending)
    {
        super(pending);
    }

    public static CompletableContextHistorySink
    of(CompletionStageMap<String,ICompletableContext<String>> pending)
    {
        return new CompletableContextHistorySink(pending);
    }

    @Override
    protected CompletionStage<ICompletableContext<String>>
    doProcess(ICompletableContext<String> context)
    {
        if (context instanceof IPipelineContext<String> pipeline)
        {
            System.out.println(
                String.format("Context: %s\nSteps: %s",
                    pipeline.getId(),
                    pipeline
                        .getAccumulatedSteps()
                        .stream()
                        .map(PipelineStep::getName)
                        .collect(Collectors.joining(" -> "))));
        }
        else
            System.out.println(String.format("Context: %s",context.getId()));

        return CompletableFuture.completedFuture(context);
    }
}

//////////////////////////////////////////////////////////////////////////////
