/// ///////////////////////////////////////////////////////////////////////////
// CompletableStreamConsumer.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.shared;

import strata.stream.core.shared.IConsumer;
import strata.stream.pipeline.concurrent.CompletableStreamStage;
import strata.stream.pipeline.concurrent.ICompletableContext;

import java.io.Serializable;

public
class CompletableStreamConsumer<T extends Serializable>
    implements IConsumer<CompletableStreamStage<T>>
{
    private IConsumer<ICompletableContext<T>> consumer;

    public
    CompletableStreamConsumer(IConsumer<ICompletableContext<T>> consumer)
    {
        this.consumer = consumer;
    }

    @Override
    public void
    accept(CompletableStreamStage<T> stage)
    {
        stage.thenAccept(consumer);
    }

    public static <T extends Serializable> CompletableStreamConsumer<T>
    of(IConsumer<ICompletableContext<T>> consumer)
    {
        return new CompletableStreamConsumer<>(consumer);
    }
}

//////////////////////////////////////////////////////////////////////////////
