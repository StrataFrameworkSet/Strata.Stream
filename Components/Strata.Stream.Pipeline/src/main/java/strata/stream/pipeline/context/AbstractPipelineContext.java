//////////////////////////////////////////////////////////////////////////////
// AbstractPipelineContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.foundation.core.utility.Conditional;
import strata.foundation.core.utility.ExtendedOptional;
import strata.stream.pipeline.concurrent.AbstractCompletableContext;
import strata.stream.pipeline.shared.PipelineException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract
class AbstractPipelineContext<T extends Serializable>
    extends AbstractCompletableContext<T>
    implements IPipelineContext<T>
{
    private String                         stepPrefix;
    private final List<PipelineStep>       previousSteps;
    private ExtendedOptional<PipelineStep> currentStep;

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
        super(value);
        this.stepPrefix = stepPrefix;
        this.previousSteps = new ArrayList<>(previousSteps);
        this.currentStep = ExtendedOptional.empty();
    }

    protected
    AbstractPipelineContext(
        String             stepPrefix,
        PipelineException  exception,
        List<PipelineStep> previousSteps)
    {
        super(null);
        this.stepPrefix = stepPrefix;
        this.previousSteps = new ArrayList<>(previousSteps);
        this.currentStep = ExtendedOptional.of(PipelineStep.of(this,stepPrefix,exception));
    }

    protected
    AbstractPipelineContext(
        String              stepPrefix,
        T                   value,
        IPipelineContext<?> previous)
    {
        super(value,previous);
        this.stepPrefix = stepPrefix;
        this.previousSteps = new ArrayList<>(previous.getAccumulatedSteps());
        this.currentStep = ExtendedOptional.empty();
    }

    protected
    AbstractPipelineContext(
        String              stepPrefix,
        PipelineException   exception,
        IPipelineContext<?> previous)
    {
        super(null,previous);
        this.stepPrefix = stepPrefix;
        this.previousSteps = new ArrayList<>(previous.getAccumulatedSteps());
        this.currentStep = ExtendedOptional.of(PipelineStep.of(this,stepPrefix,exception));
    }

    @Override
    public IPipelineContext<T>
    filterOut()
    {
        return (IPipelineContext<T>)super.filterOut();
    }

    @Override
    public IPipelineContext<T>
    setStepPrefix(String stepPrefix)
    {
        this.stepPrefix = stepPrefix;
        return this;
    }

    @Override
    public IPipelineContext<T>
    startStep(String step) throws IllegalStateException
    {
        currentStep.ifPresent(current -> throwCurrentStepExistsException(current));
        currentStep = ExtendedOptional.of(new PipelineStep(this,stepPrefix + step));
        getCurrentStep()
            .ifPresentOrElse(current ->
                getLogger().info("Starting step {}.",current.getName()),
                () -> getLogger().error("Current step not present."));
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
                getLogger().info("Completed step {}.",current.getName());
                complete(current);
                notifyCompleted(current);
                currentStep = ExtendedOptional.empty();
                break;

            case COMPLETED:
                getLogger().warn("Step {} already completed.",current.getName());
                break;

            case COMPLETED_WITH_EXCEPTION:
                getLogger().error(
                    "Step {} already completed with exception.",
                    current.getName());
                throw new IllegalStateException("Step already completed with exception.");

            case FAILED:
                getLogger().error(
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
                getLogger().info(
                    "Completed step {} with exception {}.",
                    current.getName(),
                    Objects.toString(
                        exception.getMessage(),
                        exception.getClass().getSimpleName()));
                completeWith(current,exception);
                notifyCompleted(current);
                currentStep = ExtendedOptional.empty();
                break;

            case COMPLETED_WITH_EXCEPTION:
                getLogger().warn(
                    "Step {} already completed with exception.",
                    current.getName());
                break;

            case COMPLETED:
                getLogger().error(
                    "Step {} already completed.",
                    current.getName());
                throw
                    new IllegalStateException(
                        "Step already completed.");

            case FAILED:
                getLogger().error(
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
                getLogger().error(
                    "Step {} failed with exception {}.",
                    getCurrentStep(),
                    Objects.toString(
                        exception.getMessage(),
                        exception.getClass().getSimpleName()));
                failWith(current,exception);
                notifyFailed(current);
                currentStep = ExtendedOptional.empty();
                break;

            case FAILED:
                getLogger().warn(
                    "Step {} already failed with exception.",
                    current.getName());
                break;

            case COMPLETED:
                getLogger().error(
                    "Step {} already completed.",
                    current.getName());
                throw new IllegalStateException("Step already completed.");

            case COMPLETED_WITH_EXCEPTION:
                getLogger().error(
                    "Step {} already completed with exception.",
                    current.getName());
                throw new IllegalStateException("Step already completed with exception.");
        }

        return this;
    }

    @Override
    public String
    getStepPrefix() { return stepPrefix; }

    @Override
    public Optional<PipelineStep>
    getCurrentStep()
    {
        return currentStep.toOptional();
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
        getLogger().debug(
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

    protected Logger
    getLogger()
    {
        return LogManager.getLogger(getClass());
    }

    protected void
    notifyCompleted(PipelineStep step)
    {
        switch (step.getStatus())
        {
            case COMPLETED:
                getLogger().debug(
                    "No-op method: notifyCompleted({}) was called.",
                    step.getName());
                break;

            case COMPLETED_WITH_EXCEPTION:
                getLogger().debug(
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
                getLogger().error(
                    "No-op method: notifyCompleted({}) was called but step still in-progress.",
                    step.getName());
                break;

            case FAILED:
                getLogger().error(
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
                getLogger().debug(
                    "No-op method: notifyFailed({}) was called.",
                    step.getName());
                break;

            case COMPLETED:
            case COMPLETED_WITH_EXCEPTION:
                getLogger().error(
                    "No-op method: notifyFailed({}) was called but step completed.",
                    step.getName());
                break;

            case IN_PROGRESS:
                getLogger().error(
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
