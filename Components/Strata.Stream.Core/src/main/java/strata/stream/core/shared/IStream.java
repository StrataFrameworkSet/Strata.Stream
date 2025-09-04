//////////////////////////////////////////////////////////////////////////////
// IStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.shared;

public
interface IStream<T>
    extends IStreamable
{
    IStream<T>
    filter(IPredicate<? super T> predicate);

    <R> IStream<R>
    map(IFunction<? super T,? extends R> mapper);

    <R> IStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper);
    /*

    IExtension
    forEach(IConsumer<? super T> action);

    IExtension
    sinkTo(IStreamSink<T> sink);

     */
}

//////////////////////////////////////////////////////////////////////////////