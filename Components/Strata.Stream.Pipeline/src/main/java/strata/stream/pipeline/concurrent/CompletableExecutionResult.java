/// ///////////////////////////////////////////////////////////////////////////
// CompletableExecutionResult.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import strata.stream.core.shared.IExecutionResult;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public
class CompletableExecutionResult<T extends Serializable>
    implements IExecutionResult
{
    private final IExecutionResult result;
    private final Map<String,T>    completed;

    public
    CompletableExecutionResult(
        IExecutionResult result,
        Map<String,T>    completed)
    {
        this.result    = result;
        this.completed = completed;
    }

    @Override
    public String
    getExecutionId()
    {
        return result.getExecutionId();
    }

    @Override
    public Duration
    getExecutionDurationIn(TimeUnit units)
    {
        return result.getExecutionDurationIn(units);
    }

    public Map<String,T>
    getCompleted()
    {
        return completed;
    }

    public static CompletableExecutionResult
    of(
        IExecutionResult   result,
        Map<String,Object> completed)
    {
        return new CompletableExecutionResult(result,completed);
    }
}

//////////////////////////////////////////////////////////////////////////////
