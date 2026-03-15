//////////////////////////////////////////////////////////////////////////////
// PipelineTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.main;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.foundation.core.collection.ICollection;
import strata.stream.core.shared.IExecutionResult;
import strata.stream.core.shared.IFunction;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.flink.unbounded.FlinkFlatMapFunctionAdapter;
import strata.stream.pipeline.concurrent.CompletableExecutionResult;
import strata.stream.pipeline.concurrent.CompletableStreamStage;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static strata.foundation.core.concurrent.Awaiter.await;

@Tag("CommitStage")
public
class CompletableBatchPipelineTest
{
    private IPipelineFactory<Long> factory;
    private IPipeline<Long>        pipeline;

    @BeforeAll
    public static void
    globalSetup()
    {
        FlinkFlatMapFunctionAdapter
            .registerReturnType(
                (IFunction<CompletableStreamStage<ICollection<String>>,Iterable<CompletableStreamStage<String>>>) s -> s.flatMap(list -> list),
                (Class<CompletableStreamStage<String>>)(Class<?>)CompletableStreamStage.class);

    }

    @BeforeEach
    public void
    setup()
    {
        factory = new CompletableBatchPipelineFactory();
        pipeline = factory.create(Long.class,"CompletableBatchPipeline");

        Optional<String> foo = Optional.of("foo");

        String message = foo.map(s -> s + "bar").orElse("baz");
    }

    @Test
    public void
    testExecute()
    {
        await(
            pipeline
                .execute()
                .thenApply(execution -> logStatus(execution))
                .thenAccept(execution -> logResult(execution)));

    }

    private static IStreamExecution
    logStatus(IStreamExecution execution)
    {
        System.out.println("Status = " + await(execution.getStatus()));
        return execution;
    }

    private static IStreamExecution
    logResult(IStreamExecution execution)
    {
        IExecutionResult result = await(execution.getResult());

        if (result instanceof CompletableExecutionResult<?> completable)
        {
            completable
                .getCompleted()
                .entrySet()
                .forEach(
                    entry ->
                        System
                            .out
                            .println(
                                "Result [" +
                                    entry.getKey() +
                                    "] = " +
                                    entry.getValue()));
        }

        System
            .out
            .println(
                "Duration(ms) = " +
                        result.getExecutionDurationIn(TimeUnit.MILLISECONDS)
                        .toMillis());

        return execution;
    }
}

//////////////////////////////////////////////////////////////////////////////
