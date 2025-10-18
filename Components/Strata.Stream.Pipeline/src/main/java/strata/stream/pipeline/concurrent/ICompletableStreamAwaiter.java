/// ///////////////////////////////////////////////////////////////////////////
// ICompletableStreamAwaiter.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.concurrent;

import strata.stream.core.shared.IFunction;

import java.io.Serializable;
import java.util.Map;

public
interface ICompletableStreamAwaiter<T extends Serializable>
    extends IFunction<CompletableStreamStage<T>,CompletableStreamStage<T>>
{
    Map<String,T>
    awaitAll();
}

//////////////////////////////////////////////////////////////////////////////