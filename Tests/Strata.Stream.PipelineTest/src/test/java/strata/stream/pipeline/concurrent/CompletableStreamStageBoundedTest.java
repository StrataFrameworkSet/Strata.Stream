//////////////////////////////////////////////////////////////////////////////
// CompletableStreamStageBoundedTest.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import strata.foundation.core.collection.IList;
import strata.foundation.core.collection.SerializableList;
import strata.stream.basic.bounded.BasicBoundedStream;
import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.shared.IExecutor;
import strata.stream.core.shared.SuppliedExecutor;
import strata.stream.pipeline.context.IPipelineContext;
import strata.stream.pipeline.context.PipelineContext;
import strata.stream.pipeline.enrichment.CompletableEnricherMapper;
import strata.stream.pipeline.enrichment.ICompletableEnricher;
import strata.stream.pipeline.enrichment.ICompletableEnricherMapper;
import strata.stream.pipeline.validation.CompletableValidatorFilter;
import strata.stream.pipeline.validation.ICompletableValidatorFilter;
import strata.stream.pipeline.validation.ValidationResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("CommitStage")
public
class CompletableStreamStageBoundedTest
{
    private IBoundedStream<CompletableStreamStage<String>> bounded;
    private IExecutor                                      executor;

    @BeforeEach
    public void
    setup()
    {
        IBoundedStream<String> source = BasicBoundedStream.of(List.of("A","BB","CCC"));

        executor = SuppliedExecutor.of(() -> Executors.newCachedThreadPool());

        bounded =
            source
                .map(
                    value ->
                        CompletableStreamStage.of(
                            () -> PipelineContext.of("Test",value),
                            executor));
    }

    @Test
    public void
    testMap()
    {
        bounded
            .map(stage -> stage.map(value -> value + "!"))
            .map(stage -> stage.map(value -> assertEndsWith(value,"!")))
            .map(stage -> stage.map(value -> printValue(value)))
            .forEach(stage -> stage.join());
    }

    @Test
    public void
    testFlatMap()
    {
        IBoundedStream<List<String>> source =
            BasicBoundedStream.of(
                List.of(
                    List.of("A","BB","CCC"),
                    List.of("DD","EEE","FFFFF")));
        IBoundedStream<CompletableStreamStage<IList<String>>> windowed =
            source
                .map(
                    value ->
                        CompletableStreamStage.of(
                            () -> PipelineContext.of(SerializableList.of(value)),
                            executor));

        windowed
            .flatMap(stage -> stage.flatMap(value -> value))
            .map(stage -> stage.filter(value -> !value.equals("EEE")))
            .map(stage -> stage.map(value -> value + "!"))
            .map(stage -> stage.map(value -> assertEndsWith(value,"!")))
            .map(stage -> stage.map(value -> printValue(value)))
            .forEach(stage -> stage.join());
    }

    @Test
    public void
    testFilter()
    {
        bounded
            .map(stage -> stage.filter(value -> !value.equals("BB")))
            .map(stage -> stage.map(value -> value + "!"))
            .map(stage -> stage.map(value -> assertEndsWith(value,"!")))
            .map(stage -> stage.map(value -> printValue(value)))
            .forEach(stage -> stage.join());
    }

    @Test
    public void
    testPipeline()
    {
        ICompletableValidatorFilter<String> filter =
            CompletableValidatorFilter.of(
                "Validate(!BB)",
                CompletableStringValidator.of(value -> !value.equals("BB"),executor));
        ICompletableEnricherMapper<String,String> enricher =
            CompletableEnricherMapper.of(
                "Enrich(!)",
                value -> CompletableFuture.supplyAsync(() -> value + "!",executor));

        bounded
            .map(filter)
            .map(enricher)
            .map(stage -> stage.map(value -> assertEndsWith(value,"!")))
            .map(stage -> stage.map(this::printValue))
            .map(stage -> stage.thenApply(this::printSteps))
            .forEach(stage -> stage.join());
    }

    private String
    assertEndsWith(String value, String suffix)
    {
        assertTrue(value.endsWith(suffix));

        return value;
    }

    private String
    printValue(String value)
    {
        System.out.println(value);
        return value;
    }

    private ICompletableContext<String>
    printSteps(ICompletableContext<String> context)
    {
        if (context instanceof IPipelineContext<String> pipeline)
            System.out.println(
                pipeline
                    .getAccumulatedSteps()
                    .stream()
                    .map(step -> step.getName())
                    .collect(Collectors.joining(" -> ")));
        else
            System.out.println("Not a pipeline context");

        return context;
    }
}

//////////////////////////////////////////////////////////////////////////////
