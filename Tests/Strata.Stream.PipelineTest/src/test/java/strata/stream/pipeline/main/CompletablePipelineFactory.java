/// ///////////////////////////////////////////////////////////////////////////
// CompletablePipelineFactory.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.main;

import strata.foundation.core.concurrent.CompletionStageMap;
import strata.stream.core.shared.IExecutor;
import strata.stream.core.shared.SuppliedExecutor;
import strata.stream.core.unbounded.IUnboundedStreamExecutor;
import strata.stream.core.unbounded.IUnboundedStreamSource;
import strata.stream.pipeline.concurrent.CompletableStreamAwaiter;
import strata.stream.pipeline.concurrent.CompletableStreamStage;
import strata.stream.pipeline.concurrent.CompletableUnboundedStreamExecutor;
import strata.stream.pipeline.concurrent.ICompletableContext;
import strata.stream.pipeline.context.PipelineContext;
import strata.stream.pipeline.enrichment.CompletableEnricherMapper;
import strata.stream.pipeline.enrichment.StringBangCompletableEnricher;
import strata.stream.pipeline.shared.CompletableStringLogger;
import strata.stream.pipeline.transformation.CompletableTransformerMapper;
import strata.stream.pipeline.transformation.LongToStringCompletableTransformer;
import strata.stream.pipeline.validation.CompletableValidatorFilter;
import strata.stream.pipeline.validation.GreaterThanZeroCompletableValidator;
import strata.stream.pipeline.validation.ValidationResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public
class CompletablePipelineFactory
    extends AbstractPipelineFactory<Long,IUnboundedStreamSource<Long>>
    implements ITestPipelineFactory
{
    private final CompletionStageMap<String,ICompletableContext<String>> pending;
    private final IExecutor                                              executor;

    public
    CompletablePipelineFactory()
    {
        pending = new CompletionStageMap<>();
        executor = SuppliedExecutor.of(() -> Executors.newCachedThreadPool());
    }

    @Override
    protected IUnboundedStreamSource<Long>
    getSource(Class<Long> inputType)
    {
        return
            new FromDataUnboundedStreamSource<>(
                inputType,
                -5L,-4L,-3L,-2L,-1L,0L,1L,2L,3L,4L,5L,11L,12L,13L,14L,15L);
    }

    @Override
    protected IUnboundedStreamExecutor
    configure(IUnboundedStreamSource<Long> source)
    {
        return
            CompletableUnboundedStreamExecutor.of(
                source
                    .map(
                        i ->
                            CompletableStreamStage.of(
                                () -> PipelineContext.of(i),executor))
                    .map(
                        CompletableValidatorFilter.of(
                            "Long>0",
                            GreaterThanZeroCompletableValidator.of(executor)))
                    .map(
                        CompletableTransformerMapper.of(
                            "LongToString",
                            LongToStringCompletableTransformer.of(executor)))
                    .map(
                        CompletableEnricherMapper.of(
                            "String!",
                            StringBangCompletableEnricher.of(executor)))
                    .map(new CompletableStringLogger())
                    .map(CompletableStreamAwaiter.of(pending)),
                pending);
    }
}

//////////////////////////////////////////////////////////////////////////////
