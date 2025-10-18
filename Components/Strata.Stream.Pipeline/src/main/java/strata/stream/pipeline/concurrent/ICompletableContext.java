//////////////////////////////////////////////////////////////////////////////
// ICompletableContext.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import strata.stream.core.shared.IFunction;

import java.io.Serializable;

public
interface ICompletableContext<T extends Serializable>
    extends Serializable
{
    String
    getId();

    T
    getValue() throws NullPointerException;

    boolean
    hasValue();

    boolean
    isFilteredOut();

    ICompletableContext<T>
    filterOut();

    <R extends Serializable> ICompletableContext<R>
    mapValue(IFunction<T,R> mapper);
}

//////////////////////////////////////////////////////////////////////////////