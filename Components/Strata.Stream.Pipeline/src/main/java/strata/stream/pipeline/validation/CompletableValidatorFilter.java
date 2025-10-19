//////////////////////////////////////////////////////////////////////////////
// CompletableValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import strata.stream.pipeline.concurrent.CompletableStreamStage;
import strata.stream.pipeline.concurrent.ICompletableContext;
import strata.stream.pipeline.context.IPipelineContext;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class CompletableValidatorFilter<T extends Serializable>
    implements ICompletableValidatorFilter<T>
{
    private final String                   step;
    private final ICompletableValidator<T> validator;

    public
    CompletableValidatorFilter(String step,ICompletableValidator<T> validator)
    {
        this.step = Objects.toString(step,".Validate");
        this.validator = validator;
    }

    @Override
    public CompletableStreamStage<T>
    apply(CompletableStreamStage<T> subject)
    {
        return subject.thenCompose(this::doValidate);
    }

    public static <T extends Serializable> CompletableValidatorFilter<T>
    of(String step,ICompletableValidator<T> validator)
    {
        return new CompletableValidatorFilter<>(step,validator);
    }

    private CompletionStage<ICompletableContext<T>>
    doValidate(ICompletableContext<T> context)
    {
        if (context.isFilteredOut())
            return CompletableFuture.completedFuture(context);

        if (!(context instanceof IPipelineContext<T> pipeline))
            throw new IllegalStateException(
                "CompletableValidatorFilter requires IPipelineContext<T>");

        pipeline.startStep("." + step);

        return
            validator
                .validate(pipeline.getValue())
                .thenApply(result -> processResult(result,pipeline));
    }

    private ICompletableContext<T>
    processResult(ValidationResult<T> result,IPipelineContext<T> pipeline)
    {
        return
            result.isValid()
                ? pipeline.completeStep()
                : createFailedValidation(pipeline,ValidationFailedException.of(result));
    }

    private IPipelineContext<T>
    createFailedValidation(IPipelineContext<T> context,ValidationFailedException exception)
    {
        return
            context
                .completeStepWith(exception)
                .filterOut();
    }
}


//////////////////////////////////////////////////////////////////////////////
