/// ///////////////////////////////////////////////////////////////////////////
// CompletableUnboundedStreamSink.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import strata.foundation.core.concurrent.CompletionStageMap;
import strata.stream.core.unbounded.AbstractUnboundedStreamSink;
import strata.stream.core.unbounded.IUnboundedStream;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public abstract
class CompletableUnboundedStreamSink<T extends Serializable,R extends Serializable>
    extends AbstractUnboundedStreamSink<CompletableStreamStage<T>>
{
    private CompletionStageMap<String,ICompletableContext<R>> pending;

    protected
    CompletableUnboundedStreamSink(CompletionStageMap<String,ICompletableContext<R>> pending)
    {
        this.pending = pending;
    }

    @Override
    public void
    accept(IUnboundedStream<CompletableStreamStage<T>> stream)
    {
        stream.forEach(stage -> process(stage));
    }

    protected void
    process(CompletableStreamStage<T> stage)
    {
        CompletableStreamStage<R> sunkStage =
            stage.thenCompose(this::doProcess);

        pending.put(sunkStage.getKey(),sunkStage.getContext());
    }

    protected abstract CompletionStage<ICompletableContext<R>>
    doProcess(ICompletableContext<T> context);
}

//////////////////////////////////////////////////////////////////////////////
