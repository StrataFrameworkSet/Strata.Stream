//////////////////////////////////////////////////////////////////////////////
// IPipeline.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.main;

import strata.stream.core.shared.IStreamExecution;
import strata.stream.core.unbounded.IUnboundedStreamExecutor;
import strata.stream.core.unbounded.IUnboundedStreamSource;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public
interface IPipeline<I>
    extends Serializable
{
    CompletionStage<IStreamExecution>
    execute();

    String
    getName();

    IUnboundedStreamSource<I>
    getSource();

    IUnboundedStreamExecutor
    getExecutor();

}

//////////////////////////////////////////////////////////////////////////////
