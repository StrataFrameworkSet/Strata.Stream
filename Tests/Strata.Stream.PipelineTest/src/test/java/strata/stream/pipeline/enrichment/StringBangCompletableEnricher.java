/// ///////////////////////////////////////////////////////////////////////////
// StringBangCompletableEnricher.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.enrichment;

import strata.stream.core.shared.IExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class StringBangCompletableEnricher
    implements ICompletableEnricher<String,String>
{
    private IExecutor executor;

    public
    StringBangCompletableEnricher(IExecutor executor)
    {
        this.executor = executor;
    }

    @Override
    public CompletionStage<String>
    enrich(String input)
    {
        return
            CompletableFuture.supplyAsync(
                () -> input + "!",
                executor);
    }

    public static StringBangCompletableEnricher
    of(IExecutor executor)
    {
        return new StringBangCompletableEnricher(executor);
    }
}

//////////////////////////////////////////////////////////////////////////////
