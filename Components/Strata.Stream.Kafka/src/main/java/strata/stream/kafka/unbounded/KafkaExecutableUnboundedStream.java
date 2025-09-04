//////////////////////////////////////////////////////////////////////////////
// KafkaExecutableUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.AbstractExecutableUnboundedStream;
import strata.stream.core.unbounded.IExecutionDriver;
import strata.stream.core.unbounded.IUnboundedStream;

import java.util.Properties;
import java.util.concurrent.CompletionStage;

public
class KafkaExecutableUnboundedStream<K,T>
    extends AbstractExecutableUnboundedStream<T>
{
    private final KafkaUnboundedStream<K,T> stream;
    private final Logger                    logger;

    public
    KafkaExecutableUnboundedStream(KafkaUnboundedStream<K,T> s)
    {
        stream = s;
        logger = LogManager.getLogger(getClass());
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute()
    {
        logger.debug("execute()");
        return
            new KafkaUnboundedStreamExecutor(stream.getBuilder())
                .execute();
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(IExecutionDriver driver)
    {
        logger.debug("execute({})", driver.getClass().getName());
        return
            new KafkaUnboundedStreamExecutor(stream.getBuilder())
                .execute(driver);
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(Properties properties)
    {
        logger.debug("execute({})", properties);
        return
            new KafkaUnboundedStreamExecutor(stream.getBuilder())
                .execute(properties);
    }

    @Override
    protected IUnboundedStream<T>
    getStream()
    {
        return stream;
    }
}

//////////////////////////////////////////////////////////////////////////////
