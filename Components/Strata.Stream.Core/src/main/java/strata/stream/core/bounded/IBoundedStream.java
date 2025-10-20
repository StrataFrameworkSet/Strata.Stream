//////////////////////////////////////////////////////////////////////////////
// IBoundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.bounded;

import strata.stream.core.shared.*;

import java.io.Serializable;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public
interface IBoundedStream<T>
    extends IStream<T>, Serializable
{
    <K> IKeyedBoundedStream<K,T>
    keyBy(IKeySelector<K,T> selector);

    IWindowedBoundedStream<T>
    windowBy(long count);

    @Override
    IBoundedStream<T>
    filter(IPredicate<? super T> predicate);

    @Override
    <R> IBoundedStream<R>
    map(IFunction<? super T,? extends R> mapper);

    @Override
    <R> IBoundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper);

    void
    forEach(IConsumer<? super T> action);

    void
    sinkTo(IBoundedStreamSink<T> sink);

    IBoundedStream<T>
    distinct();

    IBoundedStream<T>
    peek(IConsumer<? super T> action);

    IBoundedStream<T>
    limit(long maxSize);

    IBoundedStream<T>
    skip(long n);

    IBoundedStream<T>
    sorted();

    IBoundedStream<T>
    sorted(Comparator<? super T> comparator);

    IntStream
    mapToInt(ToIntFunction<? super T> mapper);

    LongStream
    mapToLong(ToLongFunction<? super T> mapper);

    DoubleStream
    mapToDouble(ToDoubleFunction<? super T> mapper);

    IntStream
    flatMapToInt(Function<? super T,? extends IntStream> mapper);

    LongStream
    flatMapToLong(Function<? super T,? extends LongStream> mapper);

    DoubleStream
    flatMapToDouble(Function<? super T,? extends DoubleStream> mapper);

    void
    forEachOrdered(Consumer<? super T> action);

    Object[]
    toArray();

    <A> A[]
    toArray(IntFunction<A[]> generator);

    T
    reduce(T identity,BinaryOperator<T> accumulator);

    Optional<T>
    reduce(BinaryOperator<T> accumulator);

    <U> U
    reduce(U identity,BiFunction<U,? super T,U> accumulator,BinaryOperator<U> combiner);

    <R> R
    collect(Supplier<R> supplier,BiConsumer<R,? super T> accumulator,BiConsumer<R,R> combiner);

    <R,A> R
    collect(Collector<? super T,A,R> collector);

    Optional<T>
    min(Comparator<? super T> comparator);

    Optional<T>
    max(Comparator<? super T> comparator);

    long
    count();

    boolean
    anyMatch(Predicate<? super T> predicate);

    boolean
    allMatch(Predicate<? super T> predicate);

    boolean
    noneMatch(Predicate<? super T> predicate);

    Optional<T>
    findFirst();

    Optional<T>
    findAny();

    Iterator<T>
    iterator();

    Spliterator<T>
    spliterator();

    boolean
    isParallel();

    IBoundedStream<T>
    sequential();

    IBoundedStream<T>
    parallel();

    IBoundedStream<T>
    unordered();

    IBoundedStream<T>
    onClose(Runnable closeHandler);

    void
    close();

    Stream<T>
    getImplementation();
}

//////////////////////////////////////////////////////////////////////////////