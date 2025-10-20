//////////////////////////////////////////////////////////////////////////////
// AbstractStreamSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.bounded;

import strata.stream.core.shared.IConsumer;
import strata.stream.core.shared.IFunction;
import strata.stream.core.shared.IKeySelector;
import strata.stream.core.shared.IPredicate;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.*;
import java.util.stream.*;

public abstract
class AbstractBoundedStreamSource<T>
    implements IBoundedStreamSource<T>
{
    @Override
    public <K> IKeyedBoundedStream<K,T>
    keyBy(IKeySelector<K,T> selector)
    {
        return getStream().keyBy(selector);
    }

    @Override
    public IWindowedBoundedStream<T>
    windowBy(long count)
    {
        return getStream().windowBy(count);
    }

    @Override
    public IBoundedStream<T>
    filter(IPredicate<? super T> predicate)
    {
        return getStream().filter(predicate);
    }

    @Override
    public <R> IBoundedStream<R>
    map(IFunction<? super T,? extends R> mapper)
    {
        return getStream().map(mapper);
    }

    @Override
    public IntStream
    mapToInt(ToIntFunction<? super T> mapper)
    {
        return getStream().mapToInt(mapper);
    }

    @Override
    public LongStream
    mapToLong(ToLongFunction<? super T> mapper)
    {
        return getStream().mapToLong(mapper);
    }

    @Override
    public DoubleStream
    mapToDouble(ToDoubleFunction<? super T> mapper)
    {
        return getStream().mapToDouble(mapper);
    }

    @Override
    public <R> IBoundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper)
    {
        return getStream().flatMap(mapper);
    }

    @Override
    public IntStream
    flatMapToInt(Function<? super T,? extends IntStream> mapper)
    {
        return getStream().flatMapToInt(mapper);
    }

    @Override
    public LongStream
    flatMapToLong(Function<? super T,? extends LongStream> mapper)
    {
        return getStream().flatMapToLong(mapper);
    }

    @Override
    public DoubleStream
    flatMapToDouble(Function<? super T,? extends DoubleStream> mapper)
    {
        return getStream().flatMapToDouble(mapper);
    }

    @Override
    public IBoundedStream<T>
    distinct()
    {
        return getStream().distinct();
    }

    @Override
    public IBoundedStream<T>
    sorted()
    {
        return getStream().sorted();
    }

    @Override
    public IBoundedStream<T>
    sorted(Comparator<? super T> comparator)
    {
        return getStream().sorted(comparator);
    }

    @Override
    public IBoundedStream<T>
    peek(IConsumer<? super T> action)
    {
        return getStream().peek(action);
    }

    @Override
    public IBoundedStream<T>
    limit(long maxSize)
    {
        return getStream().limit(maxSize);
    }

    @Override
    public IBoundedStream<T>
    skip(long n)
    {
        return getStream().skip(n);
    }

    @Override
    public void
    forEach(IConsumer<? super T> action)
    {
        getStream().forEach(action);
    }

    @Override
    public void
    forEachOrdered(Consumer<? super T> action)
    {
        getStream().forEachOrdered(action);
    }

    @Override
    public Object[]
    toArray()
    {
        return getStream().toArray();
    }

    @Override
    public <A> A[]
    toArray(IntFunction<A[]> generator)
    {
        return getStream().toArray(generator);
    }

    @Override
    public T
    reduce(T identity,BinaryOperator<T> accumulator)
    {
        return getStream().reduce(identity,accumulator);
    }

    @Override
    public Optional<T>
    reduce(BinaryOperator<T> accumulator)
    {
        return getStream().reduce(accumulator);
    }

    @Override
    public <U> U
    reduce(U identity,BiFunction<U,? super T,U> accumulator,BinaryOperator<U> combiner)
    {
        return getStream().reduce(identity,accumulator,combiner);
    }

    @Override
    public <R> R
    collect(Supplier<R> supplier,BiConsumer<R,? super T> accumulator,BiConsumer<R,R> combiner)
    {
        return getStream().collect(supplier,accumulator,combiner);
    }

    @Override
    public <R,A> R
    collect(Collector<? super T,A,R> collector)
    {
        return getStream().collect(collector);
    }

    @Override
    public Optional<T>
    min(Comparator<? super T> comparator)
    {
        return getStream().min(comparator);
    }

    @Override
    public Optional<T>
    max(Comparator<? super T> comparator)
    {
        return getStream().max(comparator);
    }

    @Override
    public long
    count()
    {
        return getStream().count();
    }

    @Override
    public boolean
    anyMatch(Predicate<? super T> predicate)
    {
        return getStream().anyMatch(predicate);
    }

    @Override
    public boolean
    allMatch(Predicate<? super T> predicate)
    {
        return getStream().allMatch(predicate);
    }

    @Override
    public boolean
    noneMatch(Predicate<? super T> predicate)
    {
        return getStream().noneMatch(predicate);
    }

    @Override
    public Optional<T>
    findFirst()
    {
        return getStream().findFirst();
    }

    @Override
    public Optional<T>
    findAny()
    {
        return getStream().findAny();
    }

    @Override
    public void
    sinkTo(IBoundedStreamSink<T> sink)
    {
        getStream().sinkTo(sink);
    }

    @Override
    public Iterator<T>
    iterator()
    {
        return getStream().iterator();
    }

    @Override
    public Spliterator<T>
    spliterator()
    {
        return getStream().spliterator();
    }

    @Override
    public boolean
    isParallel()
    {
        return getStream().isParallel();
    }

    @Override
    public IBoundedStream<T>
    sequential()
    {
        return getStream().sequential();
    }

    @Override
    public IBoundedStream<T>
    parallel()
    {
        return getStream().parallel();
    }

    @Override
    public IBoundedStream<T>
    unordered()
    {
        return getStream().unordered();
    }

    @Override
    public IBoundedStream<T>
    onClose(Runnable closeHandler)
    {
        return getStream().onClose(closeHandler);
    }

    @Override
    public void
    close()
    {
        getStream().close();
    }

    @Override
    public Stream<T>
    getImplementation()
    {
        return getStream().getImplementation();
    }

    protected abstract IBoundedStream<T>
    getStream();
}

//////////////////////////////////////////////////////////////////////////////
