/// ///////////////////////////////////////////////////////////////////////////
// CompletableStringLogger.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.shared;

import strata.stream.core.shared.IFunction;
import strata.stream.pipeline.concurrent.CompletableStreamStage;
import strata.stream.pipeline.concurrent.ICompletableContext;

public
class CompletableStringLogger
    implements
        IFunction<
            CompletableStreamStage<String>,CompletableStreamStage<String>>
{
    @Override
    public CompletableStreamStage<String>
    apply(CompletableStreamStage<String> stage)
    {
        return stage.thenApply(this::log);
    }

    private ICompletableContext<String>
    log(ICompletableContext<String> context)
    {
        if (!context.isFilteredOut())
            System.out.println(
                String.format(
                    "id = %s, value = %s",
                    context.getId(),
                    context.getValue()));

        return context;
    }
}

//////////////////////////////////////////////////////////////////////////////
