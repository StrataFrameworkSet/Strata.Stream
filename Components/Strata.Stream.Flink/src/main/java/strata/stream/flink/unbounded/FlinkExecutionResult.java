//////////////////////////////////////////////////////////////////////////////
// FlinkExecutionResult.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import strata.stream.core.shared.IExecutionResult;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public
class FlinkExecutionResult
    implements IExecutionResult
{
    private final JobExecutionResult result;

    public
    FlinkExecutionResult(JobExecutionResult r)
    {
        result = r;
    }

    @Override
    public String
    getExecutionId()
    {
        return
            result
                .getJobID()
                .toHexString();
    }

    @Override
    public Duration
    getExecutionDurationIn(TimeUnit units)
    {
        return
            Duration.of(
                result.getNetRuntime(units),
                units.toChronoUnit());
    }
}

//////////////////////////////////////////////////////////////////////////////
