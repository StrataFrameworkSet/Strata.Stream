/// ///////////////////////////////////////////////////////////////////////////
// BasicExecutionResult.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.unbounded;

import strata.stream.core.shared.IExecutionResult;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public
class BasicExecutionResult
    implements IExecutionResult
{
    private final String   executionId;
    private final Duration executionDuration;

    public BasicExecutionResult(String executionId,Duration executionDuration)
    {
        this.executionId = executionId;
        this.executionDuration = executionDuration;
    }

    @Override
    public String
    getExecutionId()
    {
        return executionId;
    }

    @Override
    public Duration
    getExecutionDurationIn(TimeUnit units)
    {
        return executionDuration;
    }
}

//////////////////////////////////////////////////////////////////////////////
