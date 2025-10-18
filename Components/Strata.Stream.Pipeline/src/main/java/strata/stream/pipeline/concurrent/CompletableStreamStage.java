//////////////////////////////////////////////////////////////////////////////
// CompletableStreamStage.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import strata.foundation.core.utility.DefaultIdentifierGenerator;
import strata.foundation.core.utility.IIdentifierGenerator;
import strata.stream.core.shared.IExecutor;
import strata.stream.core.shared.IFunction;
import strata.stream.core.shared.IPredicate;
import strata.stream.core.shared.ISupplier;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.StreamSupport;

import static strata.foundation.core.concurrent.Awaiter.await;

public
class CompletableStreamStage<T extends Serializable>
    implements Serializable
{
    private String                                  key;
    private CompletionStage<ICompletableContext<T>> context;
    private IExecutor                               executor;

    private static final IIdentifierGenerator generator =
        new DefaultIdentifierGenerator();


    public
    CompletableStreamStage(
        ISupplier<ICompletableContext<T>> supplier,
        IExecutor                         executor)
    {
        this(generator.getNextStringId(16),supplier,executor);
    }

    public
    CompletableStreamStage(
        CompletionStage<ICompletableContext<T>> context,
        IExecutor                               executor)
    {
        this(generator.getNextStringId(16),context,executor);
    }

    public
    CompletableStreamStage(
        ISupplier<ICompletableContext<T>> supplier,
        CompletableStreamStage<?>         previous)
    {
        this(previous.getKey(),supplier,previous.getExecutor());
    }

    public
    CompletableStreamStage(
        CompletionStage<ICompletableContext<T>> context,
        CompletableStreamStage<?>               previous)
    {
        this(previous.getKey(),context,previous.getExecutor());
    }

    public
    CompletableStreamStage(
        String                            key,
        ISupplier<ICompletableContext<T>> supplier,
        IExecutor                         executor)
    {
        this.key = key;
        this.context = CompletableFuture.supplyAsync(supplier,executor);
        this.executor = executor;
    }

    public
    CompletableStreamStage(
        String                                  key,
        CompletionStage<ICompletableContext<T>> context,
        IExecutor                               executor)
    {
        this.key = key;
        this.context = context;
        this.executor = executor;
    }


    public String
    getKey() { return key; }

    public CompletionStage<ICompletableContext<T>>
    getContext()
    {
        return context;
    }

    public IExecutor
    getExecutor()
    {
        return executor;
    }

    public CompletableStreamStage<T>
    filter(IPredicate<T> predicate)
    {
        return
            CompletableStreamStage.of(
                context.thenApply(context -> doFilter(context,predicate)),
                this);
    }

    public <R extends Serializable> CompletableStreamStage<R>
    map(IFunction<T,R> mapper)
    {
        return
            CompletableStreamStage.of(
                context.thenApply(context -> doMap(context,mapper)),
                this);
    }

    public <R extends Serializable> List<CompletableStreamStage<R>>
    flatMap(IFunction<T,Iterable<R>> mapper)
    {
        return
            await(context.thenApply(c -> doFlatMap(c,mapper)))
                .stream()
                .map(c -> CompletableStreamStage.of(() -> c,this))
                .toList();
    }

    public <U extends Serializable> CompletableStreamStage<U>
    thenApply(IFunction<ICompletableContext<T>,ICompletableContext<U>> function)
    {
        return
            CompletableStreamStage.of(
                context.thenApply(function::apply),
                this);
    }

    public <U extends Serializable> CompletableStreamStage<U>
    thenCompose(
        IFunction<
            ICompletableContext<T>,
            CompletionStage<ICompletableContext<U>>> function)
    {
        return
            CompletableStreamStage.of(
                context.thenCompose(function::apply),
                this);
    }

    public CompletableStreamStage<T>
    join()
    {
        context
            .toCompletableFuture()
            .join();

        return this;
    }

    public static <T extends Serializable> CompletableStreamStage<T>
    of(ISupplier<ICompletableContext<T>> supplier,IExecutor executor)
    {
        return new CompletableStreamStage<>(supplier,executor);
    }

    public static <T extends Serializable> CompletableStreamStage<T>
    of(CompletionStage<ICompletableContext<T>> context,IExecutor executor)
    {
        return new CompletableStreamStage<>(context,executor);
    }

    public static <T extends Serializable> CompletableStreamStage<T>
    of(ISupplier<ICompletableContext<T>> supplier,CompletableStreamStage<?> previous)
    {
        return new CompletableStreamStage<>(supplier,previous);
    }

    public static <T extends Serializable> CompletableStreamStage<T>
    of(CompletionStage<ICompletableContext<T>> context,CompletableStreamStage<?> previous)
    {
        return new CompletableStreamStage<>(context,previous);
    }

    private ICompletableContext<T>
    doFilter(ICompletableContext<T> source,IPredicate<T> predicate)
    {
        if (source.isFilteredOut())
            return source;

        return
            predicate.test(source.getValue())
                ? source
                : source.filterOut();
    }

    private <R extends Serializable> ICompletableContext<R>
    doMap(ICompletableContext<T> source,IFunction<T,R> mapper)
    {
        if (source.isFilteredOut())
            return
                source
                    .mapValue(v -> (R)null)
                    .filterOut();

        return source.mapValue(mapper);
    }

    private <R extends Serializable> List<ICompletableContext<R>>
    doFlatMap(ICompletableContext<T> source,IFunction<T,Iterable<R>> mapper)
    {
        if (source.isFilteredOut())
            return
                List.of(
                    source
                        .mapValue(v -> (R)null)
                        .filterOut());

        return
            StreamSupport
                .stream(
                    mapper
                        .apply(source.getValue())
                        .spliterator(),false)
                .map(value -> source.mapValue(v -> value))
                .toList();
    }

    private void
    writeObject(ObjectOutputStream out)
        throws IOException
    {
        out.writeObject(key);
        out.writeObject(context.toCompletableFuture().join());
        out.writeObject(executor);
    }

    private void
    readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException
    {
        key = (String)in.readObject();
        context = CompletableFuture.completedFuture((ICompletableContext<T>)in.readObject());
        executor =  (IExecutor)in.readObject();
    }
}

//////////////////////////////////////////////////////////////////////////////
