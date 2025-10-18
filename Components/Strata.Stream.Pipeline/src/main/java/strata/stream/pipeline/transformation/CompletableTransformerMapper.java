//////////////////////////////////////////////////////////////////////////////
// CompletableTransformerMapper.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.transformation;

import strata.stream.pipeline.concurrent.CompletableStreamStage;
import strata.stream.pipeline.concurrent.ICompletableContext;
import strata.stream.pipeline.context.IPipelineContext;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public
class CompletableTransformerMapper<
    I extends Serializable,
    O extends Serializable>
    implements ICompletableTransformerMapper<I,O>
{
    private final String                       step;
    private final ICompletableTransformer<I,O> transformer;

    public
    CompletableTransformerMapper(String step,ICompletableTransformer<I,O> transformer)
    {
        this.step = Objects.toString(step,"Transform");
        this.transformer = transformer;
    }

    @Override
    public CompletableStreamStage<O>
    apply(CompletableStreamStage<I> input)
    {
        return input.thenCompose(this::doTransform);
    }

    public static <I extends Serializable,O extends Serializable>
    ICompletableTransformerMapper<I,O>
    of(String step,ICompletableTransformer<I,O> transformer)
    {
        return new CompletableTransformerMapper<>(step,transformer);
    }

    private CompletionStage<ICompletableContext<O>>
    doTransform(ICompletableContext<I> context)
    {
        if (!(context instanceof IPipelineContext<I> pipeline))
            throw
                new IllegalArgumentException(
                    "CompletableTransformerMapper requires IPipelineContext<T>");

        if (pipeline.isFilteredOut())
            return
                CompletableFuture.completedFuture(
                    pipeline
                        .mapValue(input -> (O)null)
                        .filterOut());

        pipeline.startStep("." + step);

        try
        {
            return
                transformer
                    .transform(pipeline.getValue())
                    .thenApply(output -> processOutput(output,pipeline));
        }
        catch (TransformationFailedException e)
        {
            return
                CompletableFuture.completedFuture(
                    pipeline
                        .failStepWith(e)
                        .mapValue(input -> (O)null));
        }
        catch (Throwable cause)
        {
            return
                CompletableFuture.completedFuture(
                    pipeline
                        .failStepWith(
                            new TransformationFailedException(
                                "Unexpected failure during transformation",
                                cause))
                        .mapValue(input -> (O)null));
        }
    }

    private ICompletableContext<O>
    processOutput(O output,IPipelineContext<I> pipeline)
    {
        return
            pipeline
                .completeStep()
                .mapValue(input -> output);
    }
}

//////////////////////////////////////////////////////////////////////////////
