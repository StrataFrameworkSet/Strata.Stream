//////////////////////////////////////////////////////////////////////////////
// ToStringContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.foundation.core.utility.Conditional;
import strata.stream.pipeline.shared.PipelineException;

import java.util.List;

public
class ToStringContext
    extends AbstractPipelineContext<String>
    implements IPipelineContext<String>
{
    public
    ToStringContext(PipelineException exception,List<PipelineStep> previousSteps)
    {
        super("ToString",exception,previousSteps);
    }

    public
    ToStringContext(String value,List<PipelineStep> previousSteps)
    {
        super("ToString",value,previousSteps);
    }

    @Override
    public ToStringContext
    startStep(String step) throws IllegalStateException
    {
        return (ToStringContext)super.startStep(step);
    }

    @Override
    public ToStringContext
    completeStep() throws IllegalStateException
    {
        return (ToStringContext)super.completeStep();
    }

    @Override
    public ToStringContext
    completeStepWith(PipelineException exception) throws IllegalStateException
    {
        return (ToStringContext)super.completeStepWith(exception);
    }

    @Override
    public ToStringContext
    failStepWith(PipelineException exception) throws IllegalStateException
    {
        return (ToStringContext)super.failStepWith(exception);
    }

    @Override
    public Conditional
    recover(PipelineException exception)
    {
        if (exception instanceof TestException)
        {
            getLogger().info("Recovered from TestException");
            return Conditional.TRUE;
        }

        return super.recover(exception);
    }

    @Override
    public boolean
    isRecoverable(PipelineException exception)
    {
        return exception instanceof TestException;
    }
}

//////////////////////////////////////////////////////////////////////////////
