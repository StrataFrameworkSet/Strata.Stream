//////////////////////////////////////////////////////////////////////////////
// IStreamExecution.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.shared;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public
interface IStreamExecution
    extends Serializable
{
    CompletionStage<Void>
    cancel();

    CompletionStage<StreamExecutionStatus>
    getStatus();

    CompletionStage<IExecutionResult>
    getResult();
}

//////////////////////////////////////////////////////////////////////////////