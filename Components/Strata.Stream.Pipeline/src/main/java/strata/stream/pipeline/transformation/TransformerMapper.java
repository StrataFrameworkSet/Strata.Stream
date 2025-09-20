//////////////////////////////////////////////////////////////////////////////
// ProcessingContextEnricherMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;


import strata.stream.pipeline.context.IPipelineContext;
import strata.stream.pipeline.context.IPipelineContextFactory;
import strata.stream.pipeline.shared.PipelineException;

import java.io.Serializable;

public
class TransformerMapper<
    I extends Serializable,
    O extends Serializable,
    CI extends IPipelineContext<I>,
    CO extends IPipelineContext<O>>
    implements ITransformerMapper<I,O,CI,CO>
{
    private final ITransformer<I,O>                  transformer;
    private final IPipelineContextFactory<I,O,CI,CO> factory;

    public TransformerMapper(
        ITransformer<I,O>                  transformer,
        IPipelineContextFactory<I,O,CI,CO> factory)
    {
        this.transformer = transformer;
        this.factory = factory;
    }

    @Override
    public CO
    apply(CI input)
    {
        input.startStep(".Transform");

        try
        {
            CO output =
                factory.create(
                    input,
                    transformer.transform(input.getValue()));

            input.completeStep();
            return output;
        }
        catch (TransformationFailedException e)
        {
            if (input.isRecoverable(e))
                return doRecovery(input,e);

            return createFailedOutput(input,e);
        }
        catch (Throwable cause)
        {
            return createFailedOutput(
                input,
                new TransformationFailedException(cause));
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
                    new TransformationFailedException(cause));
        }
    }

    private CO
    reApply(CI input)
    {
        CO output =
            factory.create(
                input,
                transformer.transform(input.getValue()));

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
