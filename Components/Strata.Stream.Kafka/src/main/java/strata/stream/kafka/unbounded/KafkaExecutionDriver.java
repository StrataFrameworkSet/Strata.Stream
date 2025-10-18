//////////////////////////////////////////////////////////////////////////////
// KafkaExecutionDriver.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.Topology;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.IExecutionDriver;

public
class KafkaExecutionDriver
    implements IExecutionDriver
{
    @Override
    public <T> IStreamExecution
    start(T input)
    {
        if (input instanceof Topology topology)
            return new KafkaStreamExecution(topology);

        throw
            new IllegalArgumentException(
                "Unsupported input type: " + input.getClass().getName());
    }
}

//////////////////////////////////////////////////////////////////////////////
