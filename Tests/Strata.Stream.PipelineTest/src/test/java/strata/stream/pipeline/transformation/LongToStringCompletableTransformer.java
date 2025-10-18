/// ///////////////////////////////////////////////////////////////////////////
// LongToStringCompletableTransformer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;

import strata.stream.core.shared.IExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class LongToStringCompletableTransformer
    implements ICompletableTransformer<Long,String>
{
    private IExecutor executor;

    public
    LongToStringCompletableTransformer(IExecutor executor)
    {
        this.executor = executor;
    }

    @Override
    public CompletionStage<String>
    transform(Long input)
    {
        return
            CompletableFuture.supplyAsync(
                () -> Long.toString(input),
                executor);
    }

    public static LongToStringCompletableTransformer
    of(IExecutor executor)
    {
        return new LongToStringCompletableTransformer(executor);
    }
}

//////////////////////////////////////////////////////////////////////////////
