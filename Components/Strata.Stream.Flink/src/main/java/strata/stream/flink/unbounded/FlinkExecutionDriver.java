//////////////////////////////////////////////////////////////////////////////
// FlinkExecutionDriver.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.IExecutionDriver;

public
class FlinkExecutionDriver
    implements IExecutionDriver
{
    @Override
    public <T> IStreamExecution
    start(T input)
    {
        try
        {
            if (input instanceof StreamExecutionEnvironment environment)
                return new FlinkStreamExecution(environment.executeAsync());

            throw new IllegalArgumentException("Invalid input type: " + input.getClass().getName());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to start Flink execution.",e);
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
