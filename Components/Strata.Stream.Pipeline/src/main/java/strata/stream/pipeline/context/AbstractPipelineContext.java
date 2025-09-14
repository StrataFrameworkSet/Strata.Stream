/// ///////////////////////////////////////////////////////////////////////////
// AbstractPipelineContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.foundation.core.utility.Conditional;
import strata.stream.pipeline.shared.PipelineException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract
class AbstractPipelineContext<T>
    implements IPipelineContext<T>
{
    private String                 stepPrefix;
    private String                 step;
    private T                      value;
    private Optional<StepResult>   result;
    private final List<StepResult> previousResults;
    private final Logger           logger;

    @SuppressWarnings("unchecked")
    protected
    AbstractPipelineContext(String stepPrefix)
    {
        this(stepPrefix,(T)null,new ArrayList<>());
    }

    protected
    AbstractPipelineContext(String stepPrefix,T value)
    {
        this(stepPrefix,value,new ArrayList<>());
    }

    protected
    AbstractPipelineContext(
        String           stepPrefix,
        T                value,
        List<StepResult> previousResults)
    {
        this.stepPrefix = stepPrefix;
        this.step = "";
        this.value = value;
        this.result = Optional.empty();
        this.previousResults = new ArrayList<>(previousResults);
        this.logger = LogManager.getLogger(getClass());
    }

    protected
    AbstractPipelineContext(
        String            stepPrefix,
        PipelineException exception,
        List<StepResult>  previousResults)
    {
        this.stepPrefix = stepPrefix;
        this.step = "";
        this.value = null;
        this.result = Optional.of(StepResult.of(getStep(),StepStatus.FAILED,exception));
        this.previousResults = new ArrayList<>(previousResults);
        this.logger = LogManager.getLogger(getClass());
    }

    @Override
    public IPipelineContext<T>
    startStep(String step)
    {
        this.step = Objects.toString(step,"");
        logger.info("Starting step {}.",getStep());
        return this;
    }

    @Override
    public IPipelineContext<T>
    completeStep()
    {
        switch (getStatus())
        {
            case IN_PROGRESS:
                logger.info("Completed step {}.",getStep());
                result = Optional.of(StepResult.of(getStep(),StepStatus.COMPLETED));
                notifyCompleted(getStep());
                step = "";
                break;

            case COMPLETED:
                logger.warn("Step {} already completed.",getStep());
                break;

            case COMPLETED_WITH_EXCEPTION:
                logger.error("Step {} already completed with exception.",getStep());
                throw new IllegalStateException("Step already completed with exception.");

            case FAILED:
                logger.error("Step {} already failed.",getStep());
                throw new IllegalStateException("Step already failed");

            default:
                logger.error("Step {} has unknown status.",getStep());
                throw new IllegalStateException("Step in invalid state");
        }

        return this;
    }

    @Override
    public IPipelineContext<T>
    completeStepWith(PipelineException exception)
    {
        switch (getStatus())
        {
            case IN_PROGRESS:
                logger.info(
                    "Completed step {} with exception {}.",
                    getStep(),
                    Objects.toString(
                        exception.getMessage(),
                        exception.getClass().getSimpleName()));
                result = Optional.of(
                    StepResult.of(
                        getStep(),
                        StepStatus.COMPLETED_WITH_EXCEPTION,
                        exception));
                notifyCompleted(getStep(),exception);
                step = "";
                break;

            case COMPLETED_WITH_EXCEPTION:
                logger.warn("Step {} already completed with exception.",getStep());
                break;

            case COMPLETED:
                logger.error("Step {} already completed.",getStep());
                throw new IllegalStateException("Step already completed.");

            case FAILED:
                logger.error("Step {} already failed.",getStep());
                throw new IllegalStateException("Step already failed");

            default:
                logger.error("Step {} has unknown status.",getStep());
                throw new IllegalStateException("Step in invalid state");
        }

        return this;
    }

    @Override
    public IPipelineContext<T>
    failStepWith(PipelineException exception)
    {
        switch (getStatus())
        {
            case IN_PROGRESS:
                logger.error(
                    "Step {} failed with exception {}.",
                    getStep(),
                    Objects.toString(
                        exception.getMessage(),
                        exception.getClass().getSimpleName()));
                result = Optional.of(StepResult.of(getStep(),StepStatus.FAILED,exception));
                notifyFailed(getStep(),exception);
                step = "";
                break;

            case FAILED:
                logger.warn("Step {} already failed with exception.",getStep());
                break;

            case COMPLETED:
                logger.error("Step {} already completed.",getStep());
                throw new IllegalStateException("Step already completed.");

            case COMPLETED_WITH_EXCEPTION:
                logger.error("Step {} already completed with exception.",getStep());
                throw new IllegalStateException("Step already completed with exception");

            default:
                logger.error("Step {} has unknown status",getStep());
                throw new IllegalStateException("Step in invalid state");
        }

        return this;
    }

    @Override
    public String
    getStep()
    {
        return stepPrefix + step;
    }

    @Override
    public T
    getValue()
        throws NullPointerException
    {
        Objects.requireNonNull(value,"No value available");
        return value;
    }

    @Override
    public StepStatus
    getStatus()
    {
        return
            result
                .map(StepResult::getStatus)
                .orElseGet(
                    () ->
                        previousResults
                            .stream()
                            .filter(previous -> isFailed(previous))
                            .findFirst()
                            .map(StepResult::getStatus)
                            .orElse(StepStatus.IN_PROGRESS));
    }

    @Override
    public List<StepResult>
    getAccumulatedResults()
    {
        return
            result
                .map(current -> accumulateResults(current))
                .orElseGet(
                    () ->
                        accumulateResults(
                            StepResult.of(getStep(),StepStatus.IN_PROGRESS)));
    }

    @Override
    public Conditional
    recover(PipelineException exception)
    {
        logger.debug(
            "No-op method: recover({}) was called.",
            exception
                .getClass()
                .getSimpleName());
        return Conditional.FALSE;
    }

    @Override
    public boolean
    isRecoverable(PipelineException exception)
    {
        return false;
    }

    protected void
    setValue(T value)
    {
        this.value = value;
    }

    protected Logger
    getLogger()
    {
        return logger;
    }

    protected void
    notifyCompleted(String step)
    {
        logger.debug("No-op method: notifyCompleted({}) was called.",step);
    }

    protected void
    notifyCompleted(String step,PipelineException exception)
    {
        logger.debug(
            "No-op method: notifyCompleted({},{}) was called.",
            step,
            exception
                .getClass()
                .getSimpleName());
    }

    protected void
    notifyFailed(String step,PipelineException exception)
    {
        logger.debug(
            "No-op method: notifyFailed({},{}) was called.",
            step,
            exception
                .getClass()
                .getSimpleName());
    }

    private boolean
    isCompleted(StepResult result)
    {
        StepStatus status = result.getStatus();

        return status.equals(StepStatus.COMPLETED) || status.equals(StepStatus.COMPLETED_WITH_EXCEPTION);
    }

    private boolean
    isFailed(StepResult result)
    {
        StepStatus status = result.getStatus();

        return status.equals(StepStatus.FAILED);
    }

    private List<StepResult>
    accumulateResults(StepResult current)
    {
        List<StepResult> accumulated = new ArrayList<>(previousResults);

        accumulated.add(current);
        return accumulated;
    }
}

//////////////////////////////////////////////////////////////////////////////
