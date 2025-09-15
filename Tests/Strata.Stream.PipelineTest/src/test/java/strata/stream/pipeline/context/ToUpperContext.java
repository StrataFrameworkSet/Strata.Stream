//////////////////////////////////////////////////////////////////////////////
// ToUpperContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.foundation.core.utility.Conditional;
import strata.stream.pipeline.shared.PipelineException;
import strata.stream.pipeline.validation.SyntacticValidationFailedException;

import java.util.List;

public
class ToUpperContext
    extends AbstractPipelineContext<String>
    implements IPipelineContext<String>
{
    public
    ToUpperContext(PipelineException exception,List<PipelineStep> previousSteps)
    {
        super("ToUpper",exception,previousSteps);
    }

    public
    ToUpperContext(String value,List<PipelineStep> previousSteps)
    {
        super("ToUpper",value,previousSteps);
    }

    @Override
    public ToUpperContext
    startStep(String step) throws IllegalStateException
    {
        return (ToUpperContext)super.startStep(step);
    }

    @Override
    public ToUpperContext
    completeStep() throws IllegalStateException
    {
        return (ToUpperContext)super.completeStep();
    }

    @Override
    public ToUpperContext
    completeStepWith(PipelineException exception) throws IllegalStateException
    {
        return (ToUpperContext)super.completeStepWith(exception);
    }

    @Override
    public ToUpperContext
    failStepWith(PipelineException exception) throws IllegalStateException
    {
        return (ToUpperContext)super.failStepWith(exception);
    }

    @Override
    public Conditional
    recover(PipelineException exception)
    {
        if (isRecoverable(exception))
        {
            getLogger().info("Recovered from SyntacticValidationFailedException(1234).");
            setValue(getValue().toUpperCase());
            return Conditional.TRUE;
        }

        getLogger().warn(
            "Failed to recover from exception: {}.",
            exception
                .getClass()
                .getSimpleName());
        return Conditional.FALSE;
    }

    @Override
    public boolean
    isRecoverable(PipelineException exception)
    {
        return
            exception instanceof SyntacticValidationFailedException e &&
            e.getFailureCodes().contains(1234);
    }
}

//////////////////////////////////////////////////////////////////////////////
