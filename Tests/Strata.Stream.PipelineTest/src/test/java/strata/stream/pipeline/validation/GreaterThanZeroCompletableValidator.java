/// ///////////////////////////////////////////////////////////////////////////
// GreaterThanZeroCompletableValidator.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.core.shared.IExecutor;

import java.io.ObjectOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class GreaterThanZeroCompletableValidator
    implements ICompletableValidator<Long>
{
    private IExecutor executor;

    public
    GreaterThanZeroCompletableValidator(IExecutor executor)
    {
        this.executor = executor;
    }

    @Override
    public CompletionStage<ValidationResult<Long>>
    validate(Long value)
    {
        return
            CompletableFuture.supplyAsync(
                () -> ValidationResult.of(value > 0,value),
                executor);
    }

    public static GreaterThanZeroCompletableValidator
    of(IExecutor executor)
    {
        return new GreaterThanZeroCompletableValidator(executor);
    }
}

//////////////////////////////////////////////////////////////////////////////
