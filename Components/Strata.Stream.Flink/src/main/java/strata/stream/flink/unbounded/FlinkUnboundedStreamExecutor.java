//////////////////////////////////////////////////////////////////////////////
// FlinkUnboundedStreamExecutor.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.IExecutionDriver;
import strata.stream.core.unbounded.IUnboundedStreamExecutor;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class FlinkUnboundedStreamExecutor
    implements IUnboundedStreamExecutor
{
    private final StreamExecutionEnvironment environment;

    public
    FlinkUnboundedStreamExecutor(StreamExecutionEnvironment env)
    {
        environment = env;

        if (environment == null)
            throw new NullPointerException("env is null");
    }

    @Override
    public CompletionStage<IStreamExecution>
    execute()
    {
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
        return execute();
    }

}

//////////////////////////////////////////////////////////////////////////////
