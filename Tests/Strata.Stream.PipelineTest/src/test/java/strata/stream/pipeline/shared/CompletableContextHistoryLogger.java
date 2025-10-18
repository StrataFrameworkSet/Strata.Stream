/// ///////////////////////////////////////////////////////////////////////////
// CompletableContextHistoryLogger.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.shared;

import strata.stream.core.shared.IConsumer;
import strata.stream.pipeline.concurrent.ICompletableContext;
import strata.stream.pipeline.context.IPipelineContext;
import strata.stream.pipeline.context.PipelineStep;

import java.util.stream.Collectors;

public
class CompletableContextHistoryLogger
    implements IConsumer<ICompletableContext<String>>
{
    @Override
    public void
    accept(ICompletableContext<String> context)
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
    }
}

//////////////////////////////////////////////////////////////////////////////
