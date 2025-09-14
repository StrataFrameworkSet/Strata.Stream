/// ///////////////////////////////////////////////////////////////////////////
// IPipelineContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.context;

import strata.foundation.core.utility.Conditional;
import strata.stream.pipeline.shared.PipelineException;

import java.util.List;
import java.util.NoSuchElementException;

public
interface IPipelineContext<T>
{
    IPipelineContext<T>
    startStep(String step);

    IPipelineContext<T>
    completeStep();

    IPipelineContext<T>
    completeStepWith(PipelineException exception);

    IPipelineContext<T>
    failStepWith(PipelineException exception);

    Conditional
    recover(PipelineException exception);

    T
    getValue() throws NullPointerException;

    String
    getStep();

    StepStatus
    getStatus();

    List<StepResult>
    getAccumulatedResults();

    boolean
    isRecoverable(PipelineException exception);
}

//////////////////////////////////////////////////////////////////////////////