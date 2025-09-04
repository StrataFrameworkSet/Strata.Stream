//////////////////////////////////////////////////////////////////////////////
// FlinkUnboundedStreamSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import strata.stream.core.unbounded.AbstractUnboundedStreamSource;
import strata.stream.core.unbounded.IUnboundedStream;

import java.util.Optional;

public
class FlinkUnboundedStreamSource<T,S extends SourceSplit,C>
    extends AbstractUnboundedStreamSource<T>
{

    private Optional<IUnboundedStream<T>> stream;

    public
    FlinkUnboundedStreamSource(
        StreamExecutionEnvironment environment,
        String                     sourceName,
        Source<T,S,C>              source)
    {
        stream =
            Optional.of(
                new FlinkUnboundedStream<>(
                    environment.fromSource(
                        source,
                        WatermarkStrategy.forMonotonousTimestamps(),
                        sourceName)));
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
