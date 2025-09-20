//////////////////////////////////////////////////////////////////////////////
// FlinkUnboundedStreamExecutor.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.units.qual.g;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.IExecutionDriver;
import strata.stream.core.unbounded.IUnboundedStreamExecutor;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class FlinkUnboundedStreamExecutor
    implements IUnboundedStreamExecutor
{
    private final StreamExecutionEnvironment environment;
    private final Logger                     logger;

    public
    FlinkUnboundedStreamExecutor(StreamExecutionEnvironment env)
    {
        Objects.requireNonNull(env, "env is null");
        environment = env;
        logger = LogManager.getLogger(getClass());
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute()
    {
        logger.trace("execute()");
        return execute(new FlinkExecutionDriver());

            /*
            CompletableFuture
                .supplyAsync(
                    () ->
                    {
                        try
                        {
                            return
                                new FlinkStreamExecution(
                                    environment.executeAsync());
                        }
                        catch (Exception e)
                        {
                            throw new RuntimeException(e);
                        }
                    });

             */
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(IExecutionDriver driver)
    {
        logger.trace("execute({})", driver);
        return
            CompletableFuture
                .supplyAsync(
                    () ->
                    {
                        try
                        {
                            return driver.start(environment);
                        }
                        catch (Exception e)
                        {
                            throw new RuntimeException(e);
                        }
                    });
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute(Properties properties)
    {
        logger.debug("execute({})", properties);
        return execute();
    }

}

//////////////////////////////////////////////////////////////////////////////
