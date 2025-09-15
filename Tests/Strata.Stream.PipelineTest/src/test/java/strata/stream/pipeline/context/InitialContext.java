/// ///////////////////////////////////////////////////////////////////////////
// InitialContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.stream.pipeline.shared.PipelineException;

public
class InitialContext
    extends AbstractPipelineContext<Long>
    implements IPipelineContext<Long>
{
    public
    InitialContext(Long value)
    {
        super("Initial",value);
    }

    @Override
    public InitialContext
    startStep(String step) throws IllegalStateException
    {
        return (InitialContext)super.startStep(step);
    }

    @Override
    public InitialContext
    completeStep() throws IllegalStateException
    {
        return (InitialContext)super.completeStep();
    }

    @Override
    public InitialContext
    completeStepWith(PipelineException exception) throws IllegalStateException
    {
        return (InitialContext)super.completeStepWith(exception);
    }

    @Override
    public InitialContext
    failStepWith(PipelineException exception) throws IllegalStateException
    {
        return (InitialContext)super.failStepWith(exception);
    }

    public static InitialContext
    of(Long value)
    {
        return new InitialContext(value);
    }
}

//////////////////////////////////////////////////////////////////////////////
