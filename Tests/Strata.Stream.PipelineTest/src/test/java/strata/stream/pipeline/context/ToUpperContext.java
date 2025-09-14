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
    ToUpperContext(PipelineException exception,List<StepResult> previousResults)
    {
        super("ToUpper",exception,previousResults);
    }

    public
    ToUpperContext(String value,List<StepResult> previousResults)
    {
        super("ToUpper",value,previousResults);
    }

    @Override
    public ToUpperContext
    startStep(String step)
    {
        return (ToUpperContext)super.startStep(step);
    }

    @Override
    public ToUpperContext
    completeStep()
    {
        return (ToUpperContext)super.completeStep();
    }

    @Override
    public ToUpperContext
    completeStepWith(PipelineException exception)
    {
        return (ToUpperContext)super.completeStepWith(exception);
    }

    @Override
    public ToUpperContext
    failStepWith(PipelineException exception)
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
