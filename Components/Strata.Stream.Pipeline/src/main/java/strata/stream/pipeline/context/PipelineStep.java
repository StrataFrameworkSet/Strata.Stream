//////////////////////////////////////////////////////////////////////////////
// PipelineStep.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.stream.pipeline.shared.PipelineException;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public
class PipelineStep
    implements Serializable
{
    private final IPipelineContext<?> context;
    private final String name;
    private StepStatus status;
    private final Instant begin;
    private Instant end;
    private Optional<PipelineException> exception;

    public
    PipelineStep(IPipelineContext<?> context,String name)
    {
        this(context,name,StepStatus.IN_PROGRESS);
    }

    public
    PipelineStep(IPipelineContext<?> context,String name,StepStatus status)
    {
        this.context = context;
        this.name = name;
        this.status = status;
        this.begin = Instant.now();
        this.end = null;
        this.exception = Optional.empty();
    }

    public
    PipelineStep(IPipelineContext<?> context,String name,PipelineException exception)
    {
        this.context = context;
        this.name = name;
        this.status = StepStatus.FAILED;
        this.begin = Instant.now();
        this.end = Instant.now();
        this.exception = Optional.ofNullable(exception);
    }

    public PipelineStep
    complete()
    {
        this.status = StepStatus.COMPLETED;
        this.end = Instant.now();

        return this;
    }

    public PipelineStep
    completeWith(PipelineException exception)
    {
        this.status = StepStatus.COMPLETED_WITH_EXCEPTION;
        this.end = Instant.now();
        this.exception = Optional.ofNullable(exception);

        return this;
    }

    public PipelineStep
    failWith(PipelineException exception)
    {
        this.status = StepStatus.FAILED;
        this.end = Instant.now();
        this.exception = Optional.ofNullable(exception);

        return this;
    }

    public IPipelineContext<?>
    getContext()
    {
        return context;
    }

    public String
    getName()
    {
        return name;
    }

    public StepStatus
    getStatus()
    {
        return status;
    }

    public Duration
    getDuration()
    {
        if (end == null)
            return Duration.between(begin,Instant.now());

        return Duration.between(begin,end);
    }

    public Optional<PipelineException>
    getException()
    {
        return exception;
    }

    public static PipelineStep
    of(IPipelineContext<?> context,String name)
    {
        return new PipelineStep(context,name);
    }

    public static PipelineStep
    of(IPipelineContext<?> context,String name,PipelineException exception)
    {
        return new PipelineStep(context,name,exception);
    }

    public static PipelineStep
    of(IPipelineContext<?> context,String name,StepStatus status)
    {
        return new PipelineStep(context,name,status);
    }

}

//////////////////////////////////////////////////////////////////////////////
