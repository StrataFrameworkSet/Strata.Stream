/// ///////////////////////////////////////////////////////////////////////////
// PipelineTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.main;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.stream.core.shared.IStreamExecution;
import strata.stream.pipeline.enrichment.InitialContextToStringContextEnricherMapper;
import strata.stream.pipeline.transformation.ToStringContextToUpperContextTransformerMapper;
import strata.stream.pipeline.validation.InitialContextSemanticValidatorFilter;
import strata.stream.pipeline.validation.InitialContextSyntacticValidatorFilter;
import strata.stream.pipeline.validation.SemanticValidationFailedException;
import strata.stream.pipeline.validation.SyntacticValidationFailedException;

import java.util.concurrent.TimeUnit;

import static strata.foundation.core.concurrent.Awaiter.await;

@Tag("IntegrationStage")
public
class PipelineTest
{
    private IPipelineFactory<Long> factory;
    private IPipeline<Long>        pipeline;

    @BeforeEach
    public void
    setup()
    {
        factory =
            new TestPipelineFactory(
                new InitialContextSyntacticValidatorFilter(input -> isEven(input)),
                new InitialContextSemanticValidatorFilter(input -> isPositive(input)),
                new InitialContextToStringContextEnricherMapper(input -> "Value=" + input),
                new ToStringContextToUpperContextTransformerMapper(input -> input.toUpperCase()));
        pipeline = factory.create(Long.class,"TestPipeline");
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
        System
            .out
            .println(
                "Duration(ms) = " +
                    await(execution.getResult())
                        .getExecutionDurationIn(TimeUnit.MILLISECONDS)
                        .toMillis());

        return execution;
    }
}

//////////////////////////////////////////////////////////////////////////////
