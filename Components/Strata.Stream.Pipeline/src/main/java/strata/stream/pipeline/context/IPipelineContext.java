/// ///////////////////////////////////////////////////////////////////////////
// IPipelineContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.foundation.core.utility.Conditional;
import strata.stream.pipeline.shared.PipelineException;

import java.util.List;
import java.util.Optional;

public
interface IPipelineContext<T>
{
    IPipelineContext<T>
    startStep(String step) throws IllegalStateException;

    IPipelineContext<T>
    completeStep() throws IllegalStateException;

    IPipelineContext<T>
    completeStepWith(PipelineException exception) throws IllegalStateException;

    IPipelineContext<T>
    failStepWith(PipelineException exception) throws IllegalStateException;

    Conditional
    recover(PipelineException exception);

    T
    getValue() throws NullPointerException;

    Optional<PipelineStep>
    getCurrentStep();

    List<PipelineStep>
    getAccumulatedSteps();

    boolean
    isRecoverable(PipelineException exception);
}

//////////////////////////////////////////////////////////////////////////////