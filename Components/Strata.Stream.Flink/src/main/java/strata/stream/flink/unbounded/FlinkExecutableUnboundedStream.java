//////////////////////////////////////////////////////////////////////////////
// FlinkExecutableUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import strata.stream.core.unbounded.AbstractExecutableUnboundedStream;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.IExecutionDriver;

import java.util.Properties;
import java.util.concurrent.CompletionStage;

public
class FlinkExecutableUnboundedStream<T>
    extends AbstractExecutableUnboundedStream<T>
{
    private final FlinkUnboundedStream<T> stream;

    public
    FlinkExecutableUnboundedStream(FlinkUnboundedStream<T> s)
    {
        stream = s;
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute()
    {
        return
            new FlinkUnboundedStreamExecutor(stream.getEnvironment())
                .execute();
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(IExecutionDriver driver) throws Exception
    {
        return
            new FlinkUnboundedStreamExecutor(stream.getEnvironment())
                .execute(driver);
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(Properties properties)
    {
        return
            new FlinkUnboundedStreamExecutor(stream.getEnvironment())
                .execute(properties);
    }

    @Override
    protected FlinkUnboundedStream<T>
    getStream()
    {
        return stream;
    }
}

//////////////////////////////////////////////////////////////////////////////
