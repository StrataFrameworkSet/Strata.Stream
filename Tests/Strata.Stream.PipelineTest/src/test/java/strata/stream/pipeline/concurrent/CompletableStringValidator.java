/// ///////////////////////////////////////////////////////////////////////////
// CompletableStringValidator.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import strata.stream.core.shared.IExecutor;
import strata.stream.core.shared.IPredicate;
import strata.stream.pipeline.validation.ICompletableValidator;
import strata.stream.pipeline.validation.ValidationResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public
class CompletableStringValidator
    implements ICompletableValidator<String>
{
    private final IPredicate<String> predicate;
    private final IExecutor          executor;

    public
    CompletableStringValidator(IPredicate<String> predicate,IExecutor executor)
    {
        this.predicate = predicate;
        this.executor  = executor;
    }

    @Override
    public CompletionStage<ValidationResult<String>>
    validate(String value)
    {
        return
            CompletableFuture
                .supplyAsync(() -> doValidate(value),executor);
    }

    public static CompletableStringValidator
    of(IPredicate<String> predicate,IExecutor executor)
    {
        return new CompletableStringValidator(predicate,executor);
    }

    private ValidationResult<String>
    doValidate(String value)
    {
        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        return ValidationResult.of(predicate.test(value),value);
    }
}

//////////////////////////////////////////////////////////////////////////////
