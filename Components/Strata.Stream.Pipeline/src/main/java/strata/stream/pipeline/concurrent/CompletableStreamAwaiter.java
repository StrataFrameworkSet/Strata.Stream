/// ///////////////////////////////////////////////////////////////////////////
// CompletableStreamAwaiter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import strata.foundation.core.concurrent.CompletionStageMap;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public
class CompletableStreamAwaiter<T extends Serializable>
    implements ICompletableStreamAwaiter<T>
{
    private final CompletionStageMap<String,ICompletableContext<T>> pending;

    public
    CompletableStreamAwaiter()
    {
        this(new CompletionStageMap<>());
    }

    public
    CompletableStreamAwaiter(
        CompletionStageMap<String,ICompletableContext<T>> pending)
    {
        this.pending = pending;
    }

    @Override
    public CompletableStreamStage<T>
    apply(CompletableStreamStage<T> stage)
    {
        pending.put(stage.getKey(),stage.getContext());
        return stage;
    }

    @Override
    public Map<String,T>
    awaitAll()
    {
        return
            pending
                .joinAll()
                .entrySet()
                .stream()
                .map(e -> Map.entry(e.getKey(),e.getValue().getValue()))
                .collect(Collectors.toMap(Entry::getKey,Entry::getValue));
    }

    public static <T extends Serializable> CompletableStreamAwaiter<T>
    of(CompletionStageMap<String,ICompletableContext<T>> pending)
    {
        return new CompletableStreamAwaiter<>(pending);
    }

}

//////////////////////////////////////////////////////////////////////////////
