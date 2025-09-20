//////////////////////////////////////////////////////////////////////////////
// ProcessingContextEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.enrichment;


import strata.stream.pipeline.context.IPipelineContext;
import strata.stream.pipeline.context.IPipelineContextFactory;
import strata.stream.pipeline.shared.PipelineException;

import java.io.Serializable;

public
class EnricherMapper<
    I extends Serializable,
    O extends Serializable,
    CI extends IPipelineContext<I>,
    CO extends IPipelineContext<O>>
    implements IEnricherMapper<I,O,CI,CO>
{
    private final IEnricher<I,O>                     enricher;
    private final IPipelineContextFactory<I,O,CI,CO> factory;

    public
    EnricherMapper(
        IEnricher<I,O>                     enricher,
        IPipelineContextFactory<I,O,CI,CO> factory)
    {
        this.enricher = enricher;
        this.factory = factory;
    }

    @Override
    public CO
    apply(CI input)
    {
        input.startStep(".Enrich");

        try
        {
            CO output =
                factory.create(
                    input,
                    enricher.enrich(input.getValue()));

            input.completeStep();
            return output;
        }
        catch (EnrichmentFailedException e)
        {
            if (input.isRecoverable(e))
                return doRecovery(input,e);

            return createFailedOutput(input,e);
        }
        catch (Throwable cause)
        {
            return createFailedOutput(
                input,
                new EnrichmentFailedException(cause));
        }
    }

    private CO
    doRecovery(CI input,PipelineException exception)
    {
        try
        {
            return
                input
                    .recover(exception)
                    .ifTrueOrElse(
                        () -> reApply(input),
                        () -> createFailedOutput(input,exception));
        }
        catch (Throwable cause)
        {
            return
                createFailedOutput(
                    input,
                    new EnrichmentFailedException(cause));
        }
    }

    private CO
    reApply(CI input)
    {
        CO output =
            factory.create(
                input,
                enricher.enrich(input.getValue()));

        input.completeStep();
        return output;
    }

    private CO
    createFailedOutput(CI input,PipelineException exception)
    {
        CO output = factory.create(input,exception);

        output.failStepWith(exception);
        return output;
    }

}

//////////////////////////////////////////////////////////////////////////////
