//////////////////////////////////////////////////////////////////////////////
// StreamAdapter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import strata.stream.core.bounded.IBoundedStream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.*;
import java.util.stream.*;

public
class StreamAdapter<T>
    implements Stream<T>
{
    private final IBoundedStream<T> adaptee;

    public
    StreamAdapter(IBoundedStream<T> stream)
    {
        adaptee = stream;
    }

    @Override
    public Stream<T>
    filter(Predicate<? super T> predicate)
    {
        return new StreamAdapter<>(adaptee.filter(e -> predicate.test(e)));
    }

    @Override
    public <R> Stream<R>
    map(Function<? super T,? extends R> mapper)
    {
        return new StreamAdapter<>(adaptee.map(e -> mapper.apply(e)));
    }

    @Override
    public IntStream
    mapToInt(ToIntFunction<? super T> mapper)
    {
        return adaptee.mapToInt(mapper);
    }

    @Override
    public LongStream
    mapToLong(ToLongFunction<? super T> mapper)
    {
        return adaptee.mapToLong(mapper);
    }

    @Override
    public DoubleStream
    mapToDouble(ToDoubleFunction<? super T> mapper)
    {
        return adaptee.mapToDouble(mapper);
    }

    @Override
    public <R> Stream<R>
    flatMap(Function<? super T,? extends Stream<? extends R>> mapper)
    {
        return
            new StreamAdapter<>(
                adaptee.flatMap(
                    e ->
                        (Iterable<R>)
                            mapper
                                .apply(e)
                                .toList()));
    }

    @Override
    public IntStream
    flatMapToInt(Function<? super T,? extends IntStream> mapper)
    {
        return adaptee.flatMapToInt(mapper);
    }

    @Override
    public LongStream
    flatMapToLong(Function<? super T,? extends LongStream> mapper)
    {
        return adaptee.flatMapToLong(mapper);
    }

    @Override
    public DoubleStream
    flatMapToDouble(Function<? super T,? extends DoubleStream> mapper)
    {
        return adaptee.flatMapToDouble(mapper);
    }

    @Override
    public Stream<T>
    distinct()
    {
        return new StreamAdapter<>(adaptee.distinct());
    }

    @Override
    public Stream<T>
    sorted()
    {
        return new StreamAdapter<>(adaptee.sorted());
    }

    @Override
    public Stream<T>
    sorted(Comparator<? super T> comparator)
    {
        return new StreamAdapter<>(adaptee.sorted(comparator));
    }

    @Override
    public Stream<T>
    peek(Consumer<? super T> action)
    {
        return new StreamAdapter<>(adaptee.peek(e -> action.accept(e)));
    }

    @Override
    public Stream<T>
    limit(long maxSize)
    {
        return new StreamAdapter<>(adaptee.limit(maxSize));
    }

    @Override
    public Stream<T>
    skip(long n)
    {
        return new StreamAdapter<>(adaptee.skip(n));
    }

    @Override
    public void
    forEach(Consumer<? super T> action)
    {
        adaptee.forEach(e -> action.accept(e));
    }

    @Override
    public void
    forEachOrdered(Consumer<? super T> action)
    {
        adaptee.forEachOrdered(action);
    }

    @Override
    public Object[]
    toArray()
    {
        return adaptee.toArray();
    }

    @Override
    public <A> A[]
    toArray(IntFunction<A[]> generator)
    {
        return adaptee.toArray(generator);
    }

    @Override
    public T
    reduce(T identity,BinaryOperator<T> accumulator)
    {
        return adaptee.reduce(identity,accumulator);
    }

    @Override
    public Optional<T>
    reduce(BinaryOperator<T> accumulator)
    {
        return adaptee.reduce(accumulator);
    }

    @Override
    public <U> U
    reduce(U identity,BiFunction<U,? super T,U> accumulator,BinaryOperator<U> combiner)
    {
        return adaptee.reduce(identity,accumulator,combiner);
    }

    @Override
    public <R> R
    collect(Supplier<R> supplier,BiConsumer<R,? super T> accumulator,BiConsumer<R,R> combiner)
    {
        return adaptee.collect(supplier,accumulator,combiner);
    }

    @Override
    public <R,A> R
    collect(Collector<? super T,A,R> collector)
    {
        return adaptee.collect(collector);
    }

    @Override
    public Optional<T>
    min(Comparator<? super T> comparator)
    {
        return adaptee.min(comparator);
    }

    @Override
    public Optional<T>
    max(Comparator<? super T> comparator)
    {
        return adaptee.max(comparator);
    }

    @Override
    public long
    count()
    {
        return adaptee.count();
    }

    @Override
    public boolean
    anyMatch(Predicate<? super T> predicate)
    {
        return adaptee.anyMatch(predicate);
    }

    @Override
    public boolean
    allMatch(Predicate<? super T> predicate)
    {
        return adaptee.allMatch(predicate);
    }

    @Override
    public boolean
    noneMatch(Predicate<? super T> predicate)
    {
        return adaptee.noneMatch(predicate);
    }

    @Override
    public Optional<T>
    findFirst()
    {
        return adaptee.findFirst();
    }

    @Override
    public Optional<T>
    findAny()
    {
        return adaptee.findAny();
    }

    @Override
    public Iterator<T>
    iterator()
    {
        return adaptee.iterator();
    }

    @Override
    public Spliterator<T>
    spliterator()
    {
        return adaptee.spliterator();
    }

    @Override
    public boolean
    isParallel()
    {
        return adaptee.isParallel();
    }

    @Override
    public Stream<T>
    sequential()
    {
        return new StreamAdapter<>(adaptee.sequential());
    }

    @Override
    public Stream<T>
    parallel()
    {
        return new StreamAdapter<>(adaptee.parallel());
    }

    @Override
    public Stream<T>
    unordered()
    {
        return new StreamAdapter<>(adaptee.unordered());
    }

    @Override
    public Stream<T>
    onClose(Runnable closeHandler)
    {
        return new StreamAdapter<>(adaptee.onClose(closeHandler));
    }

    @Override
    public void
    close()
    {
        adaptee.close();
    }
}

//////////////////////////////////////////////////////////////////////////////
