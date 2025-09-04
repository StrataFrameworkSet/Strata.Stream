//////////////////////////////////////////////////////////////////////////////
// KafkaStreamExecution.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import strata.stream.core.shared.IExecutionResult;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.shared.StreamExecutionStatus;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class KafkaStreamExecution
    implements IStreamExecution
{
    private final KafkaStreams controller;

    public
    KafkaStreamExecution(Topology topology)
    {
        this(topology,KafkaStreamsPropertiesManager.getProperties());
    }

    public
    KafkaStreamExecution(Topology topology,Properties properties)
    {
        controller = new KafkaStreams(topology,properties);
        controller.start();
    }

    @Override
    public CompletionStage<Void>
    cancel()
    {
        controller.close();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<StreamExecutionStatus>
    getStatus()
    {
        return
            CompletableFuture
                .completedFuture(convert(controller.state()));
    }

    @Override
    public CompletionStage<IExecutionResult>
    getResult()
    {
        return CompletableFuture.completedFuture(null);
    }

    private StreamExecutionStatus
    convert(KafkaStreams.State state)
    {
        switch (state)
        {
            case CREATED:
                return StreamExecutionStatus.INITIALIZED;

            case RUNNING:
                return StreamExecutionStatus.RUNNING;

            case PENDING_SHUTDOWN:
                return StreamExecutionStatus.CANCELLING;

            case NOT_RUNNING:
                return StreamExecutionStatus.SUCCEEDED;

            case PENDING_ERROR:
            case ERROR:
                return StreamExecutionStatus.FAILED;

            case REBALANCING:
                return StreamExecutionStatus.SUSPENDED;

            default:
                throw new IllegalStateException("Should never reach this code");
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
