//////////////////////////////////////////////////////////////////////////////
// StreamAdapter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import strata.stream.basic.shared.BasicFlatMapFunctionAdapter;
import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.bounded.IBoundedStreamSink;
import strata.stream.core.bounded.ICountWindowedBoundedStream;
import strata.stream.core.bounded.IKeyedBoundedStream;
import strata.stream.core.shared.IConsumer;
import strata.stream.core.shared.IFunction;
import strata.stream.core.shared.IKeySelector;
import strata.stream.core.shared.IPredicate;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public
class BasicBoundedStream<T>
    implements IBoundedStream<T>
{
    private Stream<T> implementation;

    public
    BasicBoundedStream(Stream<T> imp)
    {
        implementation = imp;
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
        return new BasicCountWindowedBoundedStream<>(this,count);
    }

    @Override
    public IBoundedStream<T>
    filter(IPredicate<? super T> predicate)
    {
        return new BasicBoundedStream<>(implementation.filter(predicate));
    }

    @Override
    public <R> IBoundedStream<R>
    map(IFunction<? super T,? extends R> mapper)
    {
        return new BasicBoundedStream<>(implementation.map(mapper));
    }

    @Override
    public IntStream
    mapToInt(ToIntFunction<? super T> mapper)
    {
        return implementation.mapToInt(mapper);
    }

    @Override
    public LongStream
    mapToLong(ToLongFunction<? super T> mapper)
    {
        return implementation.mapToLong(mapper);
    }

    @Override
    public DoubleStream
    mapToDouble(ToDoubleFunction<? super T> mapper)
    {
        return implementation.mapToDouble(mapper);
    }

    @Override
    public <R> IBoundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper)
    {
        return
            new BasicBoundedStream<>(
                implementation.flatMap(
                    new BasicFlatMapFunctionAdapter<>(mapper)));
    }

    @Override
    public IntStream
    flatMapToInt(Function<? super T,? extends IntStream> mapper)
    {
        return implementation.flatMapToInt(mapper);
    }

    @Override
    public LongStream
    flatMapToLong(Function<? super T,? extends LongStream> mapper)
    {
        return implementation.flatMapToLong(mapper);
    }

    @Override
    public DoubleStream
    flatMapToDouble(Function<? super T,? extends DoubleStream> mapper)
    {
        return implementation.flatMapToDouble(mapper);
    }

    @Override
    public IBoundedStream<T>
    distinct()
    {
        return new BasicBoundedStream<>(implementation.distinct());
    }

    @Override
    public IBoundedStream<T>
    sorted()
    {
        return new BasicBoundedStream<>(implementation.sorted());
    }

    @Override
    public IBoundedStream<T>
    sorted(Comparator<? super T> comparator)
    {
        return new BasicBoundedStream<>(implementation.sorted(comparator));
    }

    @Override
    public IBoundedStream<T>
    peek(IConsumer<? super T> action)
    {
        return new BasicBoundedStream<>(implementation.peek(action));
    }

    @Override
    public IBoundedStream<T>
    limit(long maxSize)
    {
        return new BasicBoundedStream<>(implementation.limit(maxSize));
    }

    @Override
    public IBoundedStream<T>
    skip(long n)
    {
        return new BasicBoundedStream<>(implementation.skip(n));
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
        implementation.forEach(action);
    }

    @Override
    public void
    forEachOrdered(Consumer<? super T> action)
    {
        implementation.forEachOrdered(action);
    }

    @Override
    public Object[]
    toArray()
    {
        return implementation.toArray();
    }

    @Override
    public <A> A[]
    toArray(IntFunction<A[]> generator)
    {
        return implementation.toArray(generator);
    }

    @Override
    public T
    reduce(T identity,BinaryOperator<T> accumulator)
    {
        return implementation.reduce(identity,accumulator);
    }

    @Override
    public Optional<T>
    reduce(BinaryOperator<T> accumulator)
    {
        return implementation.reduce(accumulator);
    }

    @Override
    public <U> U
    reduce(U identity,BiFunction<U,? super T,U> accumulator,BinaryOperator<U> combiner)
    {
        return implementation.reduce(identity,accumulator,combiner);
    }

    @Override
    public <R> R
    collect(
        Supplier<R>             supplier,
        BiConsumer<R,? super T> accumulator,
        BiConsumer<R,R>         combiner)
    {
        return implementation.collect(supplier,accumulator,combiner);
    }

    @Override
    public <R,A> R
    collect(Collector<? super T,A,R> collector)
    {
        return implementation.collect(collector);
    }

    @Override
    public Optional<T>
    min(Comparator<? super T> comparator)
    {
        return implementation.min(comparator);
    }

    @Override
    public Optional<T>
    max(Comparator<? super T> comparator)
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
    anyMatch(Predicate<? super T> predicate)
    {
        return implementation.anyMatch(predicate);
    }

    @Override
    public boolean
    allMatch(Predicate<? super T> predicate)
    {
        return implementation.allMatch(predicate);
    }

    @Override
    public boolean
    noneMatch(Predicate<? super T> predicate)
    {
        return implementation.noneMatch(predicate);
    }

    @Override
    public Optional<T>
    findFirst()
    {
        return implementation.findFirst();
    }

    @Override
    public Optional<T>
    findAny()
    {
        return implementation.findAny();
    }

    @Override
    public Iterator<T>
    iterator()
    {
        return implementation.iterator();
    }

    @Override
    public Spliterator<T>
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
    public IBoundedStream<T>
    sequential()
    {
        return new BasicBoundedStream<>(implementation.sequential());
    }

    @Override
    public IBoundedStream<T>
    parallel()
    {
        return new BasicBoundedStream<>(implementation.parallel());
    }

    @Override
    public IBoundedStream<T>
    unordered()
    {
        return new BasicBoundedStream<>(implementation.unordered());
    }

    @Override
    public IBoundedStream<T>
    onClose(Runnable closeHandler)
    {
        return new BasicBoundedStream<>(implementation.onClose(closeHandler));
    }

    @Override
    public void
    close()
    {
        implementation.close();
    }

    @Override
    public Stream<T>
    getImplementation() { return implementation; }

    public List<T>
    materialize()
    {
        try
        {
            List<T> materialized =
                implementation
                    .collect(Collectors.toCollection(ArrayList::new));

            implementation = materialized.stream();
            return materialized;
        }
        catch (Exception e)
        {
            implementation = Stream.empty();
            return new ArrayList<>();
        }
    }

    public static <T> IBoundedStream<T>
    of(Stream<T> stream) { return new BasicBoundedStream<>(stream); }

    public static <T> IBoundedStream<T>
    of(Collection<T> collection) { return of(collection.stream()); }

    public static <K,T> IBoundedStream<Map.Entry<K,T>>
    of(Map<K,T> map) { return of(map.entrySet().stream()); }

    public static <K,T> IBoundedStream<K>
    ofKeys(Map<K,T> map) { return of(map.keySet().stream()); }

    public static <K,T> IBoundedStream<T>
    ofValues(Map<K,T> map) { return of(map.values().stream()); }

    private void
    writeObject(ObjectOutputStream out)
        throws IOException
    {
        out.writeObject(materialize());
    }

    @SuppressWarnings("unchecked")
    private void
    readObject(ObjectInputStream in)
    {
        List<T> materialized = null;

        try
        {
            materialized = (List<T>)in.readObject();
            implementation = materialized.stream();
        }
        catch (Exception e)
        {
            implementation = Stream.empty();
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
