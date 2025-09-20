//////////////////////////////////////////////////////////////////////////////
// FromElementsUnboundedStreamSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.main;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import strata.stream.core.unbounded.AbstractUnboundedStreamSource;
import strata.stream.core.unbounded.IUnboundedStream;
import strata.stream.flink.unbounded.FlinkUnboundedStream;

import java.util.Optional;

public
class FromDataUnboundedStreamSource<T>
    extends AbstractUnboundedStreamSource<T>
{
    private final Optional<IUnboundedStream<T>> stream;

    @SuppressWarnings("unchecked")
    public
    FromDataUnboundedStreamSource(Class<T> type,T... elements)
    {
        StreamExecutionEnvironment environment =
            LocalStreamEnvironment.createLocalEnvironment();

        environment
            .getConfig()
            .setAutoWatermarkInterval(10);

        stream =
            Optional.of(
                new FlinkUnboundedStream<>(
                    environment.fromData(elements)));
    }

    @Override
    protected IUnboundedStream<T>
    getStream()
    {
        return stream.get();
    }

    @Override
    public boolean
    hasSource()
    {
        return stream.isPresent();
    }
}

//////////////////////////////////////////////////////////////////////////////
