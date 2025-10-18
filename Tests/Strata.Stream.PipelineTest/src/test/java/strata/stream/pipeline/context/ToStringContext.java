//////////////////////////////////////////////////////////////////////////////
// ToStringContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.foundation.core.utility.Conditional;
import strata.stream.core.shared.IFunction;
import strata.stream.pipeline.concurrent.ICompletableContext;
import strata.stream.pipeline.shared.PipelineException;

import java.io.Serializable;
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

    public
    ToStringContext(String value,InitialContext previous)
    {
        super("ToString",value,previous);
    }

    @Override
    public <R extends Serializable> ICompletableContext<R>
    mapValue(IFunction<String,R> mapper)
    {
        return PipelineContext.of(mapper.apply(getValue()),this);
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
