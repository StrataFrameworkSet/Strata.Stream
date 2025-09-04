//////////////////////////////////////////////////////////////////////////////
// KafkaUnboundedStreamExecutor.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.IExecutionDriver;
import strata.stream.core.unbounded.IUnboundedStreamExecutor;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class KafkaUnboundedStreamExecutor
    implements IUnboundedStreamExecutor
{
    private final Topology topology;

    public
    KafkaUnboundedStreamExecutor(StreamsBuilder builder)
    {
        topology = builder.build();

    }
    @Override
    public CompletionStage<IStreamExecution>
    execute()
    {
        return execute(new KafkaExecutionDriver());
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(IExecutionDriver driver)
    {
        return CompletableFuture.completedFuture(driver.start(topology));
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(Properties properties)
    {
        return
            CompletableFuture
                .completedFuture(
                    new KafkaStreamExecution(topology));
    }
}

//////////////////////////////////////////////////////////////////////////////
