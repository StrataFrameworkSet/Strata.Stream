//////////////////////////////////////////////////////////////////////////////
// PipelineTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.main;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.stream.core.shared.IExecutionResult;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.pipeline.concurrent.CompletableExecutionResult;
import strata.stream.pipeline.concurrent.CompletableStreamExecution;
import strata.stream.pipeline.enrichment.InitialContextToStringContextEnricherMapper;
import strata.stream.pipeline.transformation.ToStringContextToUpperContextTransformerMapper;
import strata.stream.pipeline.validation.InitialContextSemanticValidatorFilter;
import strata.stream.pipeline.validation.InitialContextSyntacticValidatorFilter;
import strata.stream.pipeline.validation.SemanticValidationFailedException;
import strata.stream.pipeline.validation.SyntacticValidationFailedException;

import java.util.concurrent.TimeUnit;

import static strata.foundation.core.concurrent.Awaiter.await;

@Tag("CommitStage")
public
class CompletablePipelineTest
{
    private IPipelineFactory<Long> factory;
    private IPipeline<Long>        pipeline;

    @BeforeEach
    public void
    setup()
    {
        factory = new CompletablePipelineFactory();
        pipeline = factory.create(Long.class,"CompletablePipeline");
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

    private static void
    isEven(Long input)
    {
        if (input % 2 == 0)
            return;

        throw new SyntacticValidationFailedException("Not even");
    }

    private static void
    isOdd(Long input)
    {
        if (input % 2 != 0)
            return;

        throw new SyntacticValidationFailedException("Not odd");
    }

    private static void
    isPositive(Long input)
    {
        if (input > 0)
            return;

        throw new SemanticValidationFailedException("Not positive");
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
