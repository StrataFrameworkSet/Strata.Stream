//////////////////////////////////////////////////////////////////////////////
// CompletableEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.enrichment;

import strata.stream.pipeline.concurrent.CompletableStreamStage;
import strata.stream.pipeline.concurrent.ICompletableContext;
import strata.stream.pipeline.context.IPipelineContext;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class CompletableEnricherMapper<I extends Serializable,O extends Serializable>
    implements ICompletableEnricherMapper<I,O>
{
    private final String                    step;
    private final ICompletableEnricher<I,O> enricher;

    public
    CompletableEnricherMapper(String step,ICompletableEnricher<I,O> enricher)
    {
        this.step     = step;
        this.enricher = enricher;
    }

    @Override
    public CompletableStreamStage<O>
    apply(CompletableStreamStage<I> input)
    {
        return input.thenCompose(this::doEnrich);
    }

    public static <I extends Serializable,O extends Serializable>
    ICompletableEnricherMapper<I,O>
    of(String step,ICompletableEnricher<I,O> enricher)
    {
        return new CompletableEnricherMapper<>(step,enricher);
    }


    private CompletionStage<ICompletableContext<O>>
    doEnrich(ICompletableContext<I> context)
    {
        if (!(context instanceof IPipelineContext<I> pipeline))
            throw
                new IllegalArgumentException(
                    "CompletableEnricherMapper requires IPipelineContext<T>");

        if (pipeline.isFilteredOut())
            return
                CompletableFuture.completedFuture(
                    pipeline
                        .mapValue(input -> (O)null)
                        .filterOut());

        pipeline.startStep("." + step);

        try
        {
            return
                enricher
                    .enrich(pipeline.getValue())
                    .thenApply(output -> processOutput(output,pipeline));
        }
        catch (EnrichmentFailedException e)
        {
            return
                CompletableFuture.completedFuture(
                    pipeline
                        .failStepWith(e)
                        .mapValue(input -> (O)null));
        }
        catch (Throwable cause)
        {
            return
                CompletableFuture.completedFuture(
                    pipeline
                        .failStepWith(
                            new EnrichmentFailedException(
                                "Unexpected failure during enrichment",
                                cause))
                        .mapValue(input -> (O)null));
        }
    }

    private ICompletableContext<O>
    processOutput(O output,IPipelineContext<I> pipeline)
    {
        return
            pipeline
                .completeStep()
                .mapValue(input -> output);
    }

}

//////////////////////////////////////////////////////////////////////////////
