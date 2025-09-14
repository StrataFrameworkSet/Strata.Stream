/// ///////////////////////////////////////////////////////////////////////////
// StepResult.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.stream.pipeline.shared.PipelineException;

import java.util.Optional;

public
class StepResult
{
    private final String      step;
    private final StepStatus  status;
    private PipelineException exception;

    public
    StepResult(String step,StepStatus status)
    {
        this(step,status,null);
    }

    public
    StepResult(String step,StepStatus status,PipelineException exception)
    {
        this.step = step;
        this.status = status;
        this.exception = exception;
    }

    public String
    getStep() { return step; }

    public StepStatus
    getStatus() { return status; }

    public Optional<PipelineException>
    getException() { return Optional.ofNullable(exception); }

    public static StepResult
    of(String step,StepStatus status)
    {
        return new StepResult(step,status);
    }

    public static StepResult
    of(String step,StepStatus status,PipelineException exception)
    {
        return new StepResult(step,status,exception);
    }
}

//////////////////////////////////////////////////////////////////////////////
