//////////////////////////////////////////////////////////////////////////////
// KeyedStream.java
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
class BasicKeyedBoundedStream<K,T>
    implements IKeyedBoundedStream<K,T>
{
    private final Map<K,IBoundedStream<T>> keyStreams;

    public
    BasicKeyedBoundedStream(IBoundedStream<T> source,IKeySelector<K,T> selector)
    {
        keyStreams = new HashMap<>();

        source
            .collect(
                Collectors.groupingBy(
                    element -> selector.getKey(element)))
            .entrySet()
            .stream()
            .forEach(
                entry ->
                    keyStreams.put(
                        entry.getKey(),
                        BasicBoundedStream.of(entry.getValue().stream())));
    }

    protected
    BasicKeyedBoundedStream(Map<K,IBoundedStream<T>> ks)
    {
        keyStreams = new HashMap<>(ks);
    }

    @Override
    public <K> IKeyedBoundedStream<K,T>
    keyBy(IKeySelector<K,T> selector)
    {
        return new BasicKeyedBoundedStream<>(this,selector);
    }

    @Override
    public ICountWindowedBoundedStream<T>
    windowBy(long count)
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    filter(IPredicate<? super T> predicate)
    {
        Map<K,IBoundedStream<T>> streams = new HashMap<>();

        getKeyStreamsEntries()
            .forEach(
                entry ->
                    streams.put(
                        entry.getKey(),
                        entry
                            .getValue()
                            .filter(predicate)));

        return new BasicKeyedBoundedStream<>(streams);
    }

    @Override
    public <R> IBoundedStream<R>
    map(IFunction<? super T,? extends R> mapper)
    {
        Map<K,IBoundedStream<R>> streams = new HashMap<>();

        getKeyStreamsEntries()
            .forEach(
                entry ->
                    streams.put(
                        entry.getKey(),
                        entry
                            .getValue()
                            .map(mapper)));

        return new BasicKeyedBoundedStream<>(streams);
    }

    @Override
    public IntStream
    mapToInt(ToIntFunction<? super T> mapper)
    {
        return null;
    }

    @Override
    public LongStream
    mapToLong(ToLongFunction<? super T> mapper)
    {
        return null;
    }

    @Override
    public DoubleStream
    mapToDouble(ToDoubleFunction<? super T> mapper)
    {
        return null;
    }

    @Override
    public <R> IBoundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper)
    {
        Map<K,IBoundedStream<R>> streams = new HashMap<>();

        getKeyStreamsEntries()
            .forEach(
                entry ->
                    streams.put(
                        entry.getKey(),
                        entry
                            .getValue()
                            .flatMap(mapper)));

        return new BasicKeyedBoundedStream<>(streams);
    }

    @Override
    public IntStream
    flatMapToInt(Function<? super T,? extends IntStream> mapper)
    {
        return null;
    }

    @Override
    public LongStream
    flatMapToLong(Function<? super T,? extends LongStream> mapper)
    {
        return null;
    }

    @Override
    public DoubleStream
    flatMapToDouble(Function<? super T,? extends DoubleStream> mapper)
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    distinct()
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    sorted()
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    sorted(Comparator<? super T> comparator)
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    peek(IConsumer<? super T> action)
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    limit(long maxSize)
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    skip(long n)
    {
        return null;
    }

    @Override
    public void
    sinkTo(IBoundedStreamSink<T> sink)
    {
        sink.accept(this);
    }

    @Override
    public void
    forEach(IConsumer<? super T> action)
    {
        getKeyStreamsValues()
            .forEach(stream -> stream.forEach(action));
    }

    @Override
    public void
    forEachOrdered(Consumer<? super T> action)
    {
        getKeyStreamsValues()
            .forEach(stream -> stream.forEachOrdered(action));
    }

    @Override
    public Object[]
    toArray()
    {
        return new Object[0];
    }

    @Override
    public <A> A[]
    toArray(IntFunction<A[]> generator)
    {
        return null;
    }

    @Override
    public T
    reduce(T identity,BinaryOperator<T> accumulator)
    {
        return null;
    }

    @Override
    public Optional<T>
    reduce(BinaryOperator<T> accumulator)
    {
        return Optional.empty();
    }

    @Override
    public <U> U
    reduce(U identity,BiFunction<U,? super T,U> accumulator,BinaryOperator<U> combiner)
    {
        return null;
    }

    @Override
    public <R> R
    collect(Supplier<R> supplier,BiConsumer<R,? super T> accumulator,BiConsumer<R,R> combiner)
    {
        return null;
    }

    @Override
    public <R,A> R
    collect(Collector<? super T,A,R> collector)
    {
        return null;
    }

    @Override
    public Optional<T>
    min(Comparator<? super T> comparator)
    {
        return Optional.empty();
    }

    @Override
    public Optional<T>
    max(Comparator<? super T> comparator)
    {
        return Optional.empty();
    }

    @Override
    public long
    count()
    {
        return 0;
    }

    @Override
    public boolean
    anyMatch(Predicate<? super T> predicate)
    {
        return false;
    }

    @Override
    public boolean
    allMatch(Predicate<? super T> predicate)
    {
        return false;
    }

    @Override
    public boolean
    noneMatch(Predicate<? super T> predicate)
    {
        return false;
    }

    @Override
    public Optional<T>
    findFirst()
    {
        return Optional.empty();
    }

    @Override
    public Optional<T>
    findAny()
    {
        return Optional.empty();
    }

    @Override
    public Iterator<T>
    iterator()
    {
        return null;
    }

    @Override
    public Spliterator<T>
    spliterator()
    {
        return null;
    }

    @Override
    public boolean
    isParallel()
    {
        return false;
    }

    @Override
    public IBoundedStream<T>
    sequential()
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    parallel()
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    unordered()
    {
        return null;
    }

    @Override
    public IBoundedStream<T>
    onClose(Runnable closeHandler)
    {
        return null;
    }

    @Override
    public void
    close()
    {

    }

    @Override
    public Stream<T>
    getImplementation()
    {
        return new StreamAdapter<>(this);
    }

    private Stream<Map.Entry<K,IBoundedStream<T>>>
    getKeyStreamsEntries() { return keyStreams.entrySet().stream(); }

    private Stream<IBoundedStream<T>>
    getKeyStreamsValues() { return keyStreams.values().stream(); }
}

//////////////////////////////////////////////////////////////////////////////
