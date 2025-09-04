//////////////////////////////////////////////////////////////////////////////
// BasicCountWindowedBoundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.bounded.IBoundedStreamSink;
import strata.stream.core.bounded.ICountWindowedBoundedStream;
import strata.stream.core.bounded.IKeyedBoundedStream;
import strata.stream.core.shared.IConsumer;
import strata.stream.core.shared.IFunction;
import strata.stream.core.shared.IKeySelector;
import strata.stream.core.shared.IPredicate;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public
class BasicCountWindowedBoundedStream<T>
    implements ICountWindowedBoundedStream<T>
{
    private final IBoundedStream<IBoundedStream<T>> implementation;

    public 
    BasicCountWindowedBoundedStream(IBoundedStream<T> stream,long count)
    {
        Set<IBoundedStream<T>> countStreams = new HashSet<>();

        for (int i=0;i<stream.count();i+=count)
        {
            countStreams.add(stream.limit(count));
            stream.skip(count);
        }

        implementation = BasicBoundedStream.of(countStreams);
    }

    @Override
    public <K> IKeyedBoundedStream<K,IBoundedStream<T>>
    keyBy(IKeySelector<K,IBoundedStream<T>> selector)
    {
        return new BasicKeyedBoundedStream<>(this,selector);
    }

    @Override
    public ICountWindowedBoundedStream<IBoundedStream<T>>
    windowBy(long count)
    {
        return new BasicCountWindowedBoundedStream<>(this,count);
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    filter(IPredicate<? super IBoundedStream<T>> predicate)
    {
        return
            new BasicBoundedStream<>(
                implementation
                    .filter(predicate)
                    .getImplementation());
    }

    @Override
    public <R> IBoundedStream<R>
    map(IFunction<? super IBoundedStream<T>,? extends R> mapper)
    {
         return
             new BasicBoundedStream<>(
                 ((IBoundedStream<R>)implementation.map(mapper))
                     .getImplementation());
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super IBoundedStream<T>> mapper)
    {
        return null;
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super IBoundedStream<T>> mapper)
    {
        return null;
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super IBoundedStream<T>> mapper)
    {
        return null;
    }

    @Override
    public <R> IBoundedStream<R>
    flatMap(IFunction<IBoundedStream<T>,Iterable<R>> mapper)
    {
        return implementation.flatMap(mapper);
    }

    @Override
    public IntStream flatMapToInt(Function<? super IBoundedStream<T>,? extends IntStream> mapper)
    {
        return null;
    }

    @Override
    public LongStream flatMapToLong(Function<? super IBoundedStream<T>,? extends LongStream> mapper)
    {
        return null;
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super IBoundedStream<T>,? extends DoubleStream> mapper)
    {
        return null;
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    distinct()
    {
        return new BasicBoundedStream<>(implementation.distinct().getImplementation());
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    sorted()
    {
        return new BasicBoundedStream<>(implementation.sorted().getImplementation());
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    sorted(Comparator<? super IBoundedStream<T>> comparator)
    {
        return new BasicBoundedStream<>(implementation.sorted(comparator).getImplementation());
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    peek(IConsumer<? super IBoundedStream<T>> action)
    {
        return new BasicBoundedStream<>(implementation.peek(action).getImplementation());
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    limit(long maxSize)
    {
        return new BasicBoundedStream<>(implementation.limit(maxSize).getImplementation());
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    skip(long n)
    {
        return new BasicBoundedStream<>(implementation.skip(n).getImplementation());
    }

    @Override
    public void
    forEach(IConsumer<? super IBoundedStream<T>> action)
    {
        implementation.forEach(action);
    }

    @Override
    public void
    forEachOrdered(Consumer<? super IBoundedStream<T>> action)
    {
        implementation.forEachOrdered(action);
    }

    @Override
    public Object[] toArray()
    {
        return new Object[0];
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator)
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    reduce(IBoundedStream<T> identity,BinaryOperator<IBoundedStream<T>> accumulator)
    {
        return new BasicBoundedStream<>(implementation.reduce(identity,accumulator).getImplementation());
    }

    @Override
    public Optional<IBoundedStream<T>>
    reduce(BinaryOperator<IBoundedStream<T>> accumulator)
    {
        return implementation.reduce(accumulator);
    }

    @Override
    public <U> U
    reduce(
        U                                  identity,
        BiFunction<U,? super IBoundedStream<T>,U> accumulator,
        BinaryOperator<U>                  combiner)
    {
        return implementation.reduce(identity,accumulator,combiner);
    }

    @Override
    public <R> R
    collect(Supplier<R> supplier,BiConsumer<R,? super IBoundedStream<T>> accumulator,BiConsumer<R,R> combiner)
    {
        return implementation.collect(supplier,accumulator,combiner);
    }

    @Override
    public <R,A> R
    collect(Collector<? super IBoundedStream<T>,A,R> collector)
    {
        return implementation.collect(collector);
    }

    @Override
    public Optional<IBoundedStream<T>>
    min(Comparator<? super IBoundedStream<T>> comparator)
    {
        return implementation.min(comparator);
    }

    @Override
    public Optional<IBoundedStream<T>>
    max(Comparator<? super IBoundedStream<T>> comparator)
    {
        return implementation.max(comparator);
    }

    @Override
    public long
    count()
    {
        return implementation.count();
    }

    @Override
    public boolean
    anyMatch(Predicate<? super IBoundedStream<T>> predicate)
    {
        return implementation.anyMatch(predicate);
    }

    @Override
    public boolean
    allMatch(Predicate<? super IBoundedStream<T>> predicate)
    {
        return implementation.allMatch(predicate);
    }

    @Override
    public boolean
    noneMatch(Predicate<? super IBoundedStream<T>> predicate)
    {
        return implementation.noneMatch(predicate);
    }

    @Override
    public Optional<IBoundedStream<T>>
    findFirst()
    {
        return implementation.findFirst();
    }

    @Override
    public Optional<IBoundedStream<T>>
    findAny()
    {
        return implementation.findAny();
    }

    @Override
    public void
    sinkTo(IBoundedStreamSink<IBoundedStream<T>> sink)
    {
        sink.accept(this);
    }

    @Override
    public Iterator<IBoundedStream<T>>
    iterator()
    {
        return implementation.iterator();
    }

    @Override
    public Spliterator<IBoundedStream<T>>
    spliterator()
    {
        return implementation.spliterator();
    }

    @Override
    public boolean
    isParallel()
    {
        return implementation.isParallel();
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    sequential()
    {
        return new BasicBoundedStream<>(implementation.sequential().getImplementation());
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    parallel()
    {
        return new BasicBoundedStream<>(implementation.parallel().getImplementation());
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    unordered()
    {
        return new BasicBoundedStream<>(implementation.unordered().getImplementation());
    }

    @Override
    public IBoundedStream<IBoundedStream<T>>
    onClose(Runnable closeHandler)
    {
        return new BasicBoundedStream<>(implementation.onClose(closeHandler).getImplementation());
    }

    @Override
    public void
    close()
    {
        implementation.close();
    }

    @Override
    public Stream<IBoundedStream<T>> getImplementation()
    {
        return null;
    }
}

//////////////////////////////////////////////////////////////////////////////
