/// ///////////////////////////////////////////////////////////////////////////
// GreaterThanZeroCompletableValidator.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.foundation.core.collection.ICollection;
import strata.stream.core.shared.IExecutor;
import strata.stream.core.shared.StreamCollector;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class GreaterThanZeroCompletableBatchValidator
    implements ICompletableBatchValidator<Long,ICollection<Long>>
{
    private IExecutor executor;

    public
    GreaterThanZeroCompletableBatchValidator(IExecutor executor)
    {
        this.executor = executor;
    }

    @Override
    public CompletionStage<ICollection<ValidationResult<Long>>>
    validate(ICollection<Long> values)
    {
        return
            CompletableFuture.supplyAsync(
                () ->
                    values
                        .stream()
                        .map(value -> ValidationResult.of(value > 0,value))
                        .collect(StreamCollector.toList()),
                executor);
    }

    public static GreaterThanZeroCompletableBatchValidator
    of(IExecutor executor)
    {
        return new GreaterThanZeroCompletableBatchValidator(executor);
    }
}

//////////////////////////////////////////////////////////////////////////////
