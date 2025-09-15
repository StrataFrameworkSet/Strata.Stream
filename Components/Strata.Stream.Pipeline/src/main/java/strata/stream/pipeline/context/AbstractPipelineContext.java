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
    private String                   stepPrefix;
    private T                        value;
    private final List<PipelineStep> previousSteps;
    private Optional<PipelineStep>   currentStep;
    private final Logger             logger;

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
        String             stepPrefix,
        T                  value,
        List<PipelineStep> previousSteps)
    {
        this.stepPrefix = stepPrefix;
        this.value = value;
        this.previousSteps = new ArrayList<>(previousSteps);
        this.currentStep = Optional.empty();
        this.logger = LogManager.getLogger(getClass());
    }

    protected
    AbstractPipelineContext(
        String             stepPrefix,
        PipelineException  exception,
        List<PipelineStep> previousSteps)
    {
        this.stepPrefix = stepPrefix;
        this.value = null;
        this.previousSteps = new ArrayList<>(previousSteps);
        this.currentStep = Optional.of(PipelineStep.of(this,stepPrefix,exception));
        this.logger = LogManager.getLogger(getClass());
    }

    @Override
    public IPipelineContext<T>
    startStep(String step) throws IllegalStateException
    {
        currentStep.ifPresent(current -> throwCurrentStepExistsException(current));
        currentStep = Optional.of(new PipelineStep(this,stepPrefix + step));
        logger.info("Starting step {}.",getCurrentStep());
        return this;
    }

    @Override
    public IPipelineContext<T>
    completeStep() throws IllegalStateException
    {
        PipelineStep current =
            currentStep.orElseThrow(
                () -> new IllegalStateException("No current step."));

        switch (current.getStatus())
        {
            case IN_PROGRESS:
                logger.info("Completed step {}.",current.getName());
                complete(current);
                notifyCompleted(current);
                currentStep = Optional.empty();
                break;

            case COMPLETED:
                logger.warn("Step {} already completed.",current.getName());
                break;

            case COMPLETED_WITH_EXCEPTION:
                logger.error(
                    "Step {} already completed with exception.",
                    current.getName());
                throw new IllegalStateException("Step already completed with exception.");

            case FAILED:
                logger.error(
                    "Step {} already failed.",
                    current.getName());
                throw new IllegalStateException("Step already failed.");
        }

        return this;
    }

    @Override
    public IPipelineContext<T>
    completeStepWith(PipelineException exception) throws IllegalStateException
    {
        PipelineStep current =
            currentStep.orElseThrow(
                () -> new IllegalStateException("No current step."));

        switch (current.getStatus())
        {
            case IN_PROGRESS:
                logger.info(
                    "Completed step {} with exception {}.",
                    current.getName(),
                    Objects.toString(
                        exception.getMessage(),
                        exception.getClass().getSimpleName()));
                completeWith(current,exception);
                notifyCompleted(current);
                currentStep = Optional.empty();
                break;

            case COMPLETED_WITH_EXCEPTION:
                logger.warn(
                    "Step {} already completed with exception.",
                    current.getName());
                break;

            case COMPLETED:
                logger.error(
                    "Step {} already completed.",
                    current.getName());
                throw
                    new IllegalStateException(
                        "Step already completed.");

            case FAILED:
                logger.error(
                    "Step {} already failed.",
                    current.getName());
                throw
                    new IllegalStateException(
                        "Step already failed.");
        }

        return this;
    }

    @Override
    public IPipelineContext<T>
    failStepWith(PipelineException exception) throws IllegalStateException
    {
        PipelineStep current =
            currentStep.orElseThrow(
                () -> new IllegalStateException("No current step."));

        switch (current.getStatus())
        {
            case IN_PROGRESS:
                logger.error(
                    "Step {} failed with exception {}.",
                    getCurrentStep(),
                    Objects.toString(
                        exception.getMessage(),
                        exception.getClass().getSimpleName()));
                failWith(current,exception);
                notifyFailed(current);
                currentStep = Optional.empty();
                break;

            case FAILED:
                logger.warn(
                    "Step {} already failed with exception.",
                    current.getName());
                break;

            case COMPLETED:
                logger.error(
                    "Step {} already completed.",
                    current.getName());
                throw new IllegalStateException("Step already completed.");

            case COMPLETED_WITH_EXCEPTION:
                logger.error(
                    "Step {} already completed with exception.",
                    current.getName());
                throw new IllegalStateException("Step already completed with exception.");
        }

        return this;
    }

    @Override
    public Optional<PipelineStep>
    getCurrentStep()
    {
        return currentStep;
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
    public List<PipelineStep>
    getAccumulatedSteps()
    {
        return
            currentStep
                .map(current -> accumulateSteps(current))
                .orElseGet(() -> previousSteps);
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
    notifyCompleted(PipelineStep step)
    {
        switch (step.getStatus())
        {
            case COMPLETED:
                logger.debug(
                    "No-op method: notifyCompleted({}) was called.",
                    step.getName());
                break;

            case COMPLETED_WITH_EXCEPTION:
                logger.debug(
                    "No-op method: notifyCompleted({}, {}) was called.",
                    step.getName(),
                    step
                        .getException()
                        .map(exception -> exception
                            .getClass()
                            .getSimpleName())
                        .orElse("missing exception"));
                break;

            case IN_PROGRESS:
                logger.error(
                    "No-op method: notifyCompleted({}) was called but step still in-progress.",
                    step.getName());
                break;

            case FAILED:
                logger.error(
                    "No-op method: notifyCompleted({}) was called but step failed.",
                    step.getName());
                break;
        }
    }

    protected void
    notifyFailed(PipelineStep step)
    {
        switch (step.getStatus())
        {
            case FAILED:
                logger.debug(
                    "No-op method: notifyFailed({}) was called.",
                    step.getName());
                break;

            case COMPLETED:
            case COMPLETED_WITH_EXCEPTION:
                logger.error(
                    "No-op method: notifyFailed({}) was called but step completed.",
                    step.getName());
                break;

            case IN_PROGRESS:
                logger.error(
                    "No-op method: notifyFailed({}) was called but step still in-progress.",
                    step.getName());
                break;
        }
    }

    private void
    throwCurrentStepExistsException(PipelineStep current)
    {
        throw
            new IllegalStateException(
                String.format("Current step: %s already exists.",current.getName()));
    }

    private void
    complete(PipelineStep current)
    {
        current.complete();
        previousSteps.add(current);
    }

    private void
    completeWith(PipelineStep current,PipelineException exception)
    {
        current.completeWith(exception);
        previousSteps.add(current);
    }

    private void
    failWith(PipelineStep current,PipelineException exception)
    {
        current.failWith(exception);
        previousSteps.add(current);
    }

    private boolean
    isFailed(PipelineStep step)
    {
        StepStatus status = step.getStatus();

        return status.equals(StepStatus.FAILED);
    }

    private List<PipelineStep>
    accumulateSteps(PipelineStep current)
    {
        List<PipelineStep> accumulated = new ArrayList<>(previousSteps);

        if (!accumulated.stream().anyMatch(previous -> current == previous))
            accumulated.add(current);

        return accumulated;
    }

    private StepStatus
    getPreviousStatus()
    {
        return
            previousSteps.isEmpty()
                ? StepStatus.IN_PROGRESS
                : previousSteps
                    .getLast()
                    .getStatus();
    }

    private boolean
    hasNonDuplicatedSteps()
    {
        return
            currentStep
                .map(
                    current ->
                        previousSteps
                            .stream()
                            .anyMatch(previous -> current == previous))
                .orElse(false);
    }
}

//////////////////////////////////////////////////////////////////////////////
