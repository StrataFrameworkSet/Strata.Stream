//////////////////////////////////////////////////////////////////////////////
// CompletableValidatorFilter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.foundation.core.collection.ICollection;
import strata.stream.core.shared.StreamCollector;
import strata.stream.pipeline.concurrent.CompletableStreamStage;
import strata.stream.pipeline.concurrent.ICompletableContext;
import strata.stream.pipeline.context.IPipelineContext;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class CompletableBatchValidatorFilter<
    T extends Serializable,
    C extends ICollection<T>>
    implements ICompletableBatchValidatorFilter<T,C>
{
    private final String                          step;
    private final ICompletableBatchValidator<T,C> validator;

    public
    CompletableBatchValidatorFilter(String step,ICompletableBatchValidator<T,C> validator)
    {
        this.step = Objects.toString(step,".Validate");
        this.validator = validator;
    }

    @Override
    public CompletableStreamStage<C>
    apply(CompletableStreamStage<C> subject)
    {
        return subject.thenCompose(this::doValidate);
    }

    public static <
        T extends Serializable,
        C extends ICollection<T>> CompletableBatchValidatorFilter<T,C>
    of(String step,ICompletableBatchValidator<T,C> validator)
    {
        return new CompletableBatchValidatorFilter<>(step,validator);
    }

    private CompletionStage<ICompletableContext<C>>
    doValidate(ICompletableContext<C> context)
    {
        if (context.isFilteredOut())
            return CompletableFuture.completedFuture(context);

        if (!(context instanceof IPipelineContext<C> pipeline))
            throw new IllegalStateException(
                "CompletableValidatorFilter requires IPipelineContext<T>");

        pipeline.startStep("." + step);

        return
            validator
                .validate(pipeline.getValue())
                .thenApply(result -> processResult(result,pipeline));
    }

    private ICompletableContext<C>
    processResult(ICollection<ValidationResult<T>> results,IPipelineContext<C> pipeline)
    {
        Class<C> collectionType = (Class<C>)pipeline.getValue().getClass();
        Set<Integer> codes = new HashSet<>();
        C validElements =
            results
                .stream()
                .filter(result -> isValid(result,codes))
                .map(ValidationResult::getSubject)
                .collect(StreamCollector.toCollection(collectionType));

        return
            validElements.isEmpty()
                ? createFailedValidation(
                    pipeline,
                    ValidationFailedException.of(
                        ValidationResult.of(false,codes,pipeline.getValue())))
                : pipeline
                    .completeStep()
                    .mapValue(ignore -> validElements);
    }

    private IPipelineContext<C>
    createFailedValidation(IPipelineContext<C> context,ValidationFailedException exception)
    {
        return
            context
                .completeStepWith(exception)
                .filterOut();
    }

    private boolean
    isValid(ValidationResult<T> result,Set<Integer> codes)
    {
        if (result.isValid())
            return true;

        getLogger().warn("Filtering out: {}",result.getSubject());
        codes.addAll(result.getCodes());
        return false;
    }

    private Logger
    getLogger() { return LogManager.getLogger(getClass()); }
}


//////////////////////////////////////////////////////////////////////////////
