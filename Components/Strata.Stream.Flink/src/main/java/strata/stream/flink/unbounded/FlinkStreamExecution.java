//////////////////////////////////////////////////////////////////////////////
// FlinkStreamExecution.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import strata.stream.core.shared.IExecutionResult;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.shared.StreamExecutionStatus;

import java.util.concurrent.CompletionStage;

public
class FlinkStreamExecution
    implements IStreamExecution
{
    private final JobClient job;

    public
    FlinkStreamExecution(JobClient j)
    {
        job = j;
    }

    @Override
    public CompletionStage<Void>
    cancel()
    {
        return job.cancel();
    }

    @Override
    public CompletionStage<StreamExecutionStatus>
    getStatus()
    {
        return
            job
                .getJobStatus()
                .thenApply(status -> convertStatus(status));
    }

    @Override
    public CompletionStage<IExecutionResult>
    getResult()
    {
        return
            job
                .getJobExecutionResult()
                .thenApply(result -> new FlinkExecutionResult(result));
    }

    private StreamExecutionStatus
    convertStatus(JobStatus status)
    {
        switch (status)
        {
            case INITIALIZING:
                return StreamExecutionStatus.INITIALIZING;

            case CREATED:
                return StreamExecutionStatus.INITIALIZED;

            case RECONCILING:
            case RESTARTING:
            case RUNNING:
                return StreamExecutionStatus.RUNNING;

            case FAILING:
            case FAILED:
                return StreamExecutionStatus.FAILED;

            case FINISHED:
                return StreamExecutionStatus.SUCCEEDED;

            case CANCELLING:
                return StreamExecutionStatus.CANCELLING;

            case CANCELED:
                return StreamExecutionStatus.CANCELLED;

            case SUSPENDED:
                return StreamExecutionStatus.SUSPENDED;

            default:
                throw
                    new IllegalStateException(
                        "Should never reach this point.");
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
